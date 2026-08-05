#include "asterrpc/registry/ZooKeeperClient.h"

#include <zookeeper/zookeeper.h>

#include <algorithm>
#include <climits>
#include <condition_variable>
#include <deque>
#include <map>
#include <mutex>
#include <thread>
#include <utility>
#include <vector>

namespace asterrpc::registry{
namespace{

std::string ErrorMessage(const std::string& operation,int code){
    const char* description=zerror(code);
    return operation+" failed: "+
           (description==nullptr?"unknown ZooKeeper error":description);
}

void ValidateOptions(const ZooKeeperClientOptions& options){
    if(options.servers.empty()){
        throw std::invalid_argument(
            "ZooKeeper servers must not be empty"
        );
    }
    if(options.session_timeout<=std::chrono::milliseconds::zero()){
        throw std::invalid_argument(
            "ZooKeeper session timeout must be positive"
        );
    }
    if(options.reconnect_delay<=std::chrono::milliseconds::zero()){
        throw std::invalid_argument(
            "ZooKeeper reconnect delay must be positive"
        );
    }
    if(options.session_timeout.count()>INT_MAX){
        throw std::invalid_argument(
            "ZooKeeper session timeout is too large"
        );
    }
}

std::vector<std::string> ParentPaths(const std::string& path){
    if(path.empty()||path.front()!='/'){
        throw std::invalid_argument(
            "ZooKeeper path must be absolute"
        );
    }
    if(path.size()>1&&path.back()=='/'){
        throw std::invalid_argument(
            "ZooKeeper path must not end with '/'"
        );
    }

    std::vector<std::string> paths;
    for(std::size_t index=1;index<path.size();++index){
        if(path[index]=='/'){
            if(index==1||path[index-1]=='/'){
                throw std::invalid_argument(
                    "ZooKeeper path contains an empty segment"
                );
            }
            paths.push_back(path.substr(0,index));
        }
    }

    if(path.size()>1){
        paths.push_back(path);
    }
    return paths;
}

void ValidateNodePrefix(const std::string& path){
    if(path.empty()||path.front()!='/'||path.back()=='/'){
        throw std::invalid_argument(
            "ZooKeeper node prefix must be an absolute path"
        );
    }
    if(path.find("//")!=std::string::npos){
        throw std::invalid_argument(
            "ZooKeeper node prefix contains an empty segment"
        );
    }
}

}

ZooKeeperError::ZooKeeperError(std::string operation,int code)
    :std::runtime_error(ErrorMessage(operation,code)),
     code_(code){}

int ZooKeeperError::Code()const noexcept{
    return code_;
}

class ZooKeeperClient::Impl{
public:
    explicit Impl(ZooKeeperClientOptions options)
        :options_(std::move(options)),
         bridge_(std::make_shared<WatcherBridge>()){
        ValidateOptions(options_);
        bridge_->owner=this;
    }

    ~Impl(){
        Close();

        std::lock_guard<std::mutex> lock(bridge_->mutex);
        bridge_->owner=nullptr;
    }

    void Start(){
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if(running_){
                return;
            }
        }

        if(worker_.joinable()){
            if(worker_.get_id()==std::this_thread::get_id()){
                throw std::logic_error(
                    "ZooKeeper client cannot restart in state callback"
                );
            }
            worker_.join();
        }

        {
            std::lock_guard<std::mutex> lock(mutex_);
            running_=true;
            reconnect_requested_=true;
            SetStateInLock(
                ZooKeeperConnectionState::Connecting,
                generation_
            );
        }

        worker_=std::thread([this](){
            WorkerLoop();
        });
        condition_.notify_all();
    }

    void Close()noexcept{
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if(!running_&&!worker_.joinable()){
                return;
            }

            running_=false;
            reconnect_requested_=false;
            events_.clear();
            tasks_.clear();
            state_=ZooKeeperConnectionState::Stopped;
        }
        condition_.notify_all();

        if(worker_.joinable()&&
           worker_.get_id()!=std::this_thread::get_id()){
            worker_.join();
        }

        {
            std::lock_guard<std::mutex> lock(mutex_);
            session_.reset();
            connected_condition_.notify_all();
        }
    }

    bool IsConnected()const noexcept{
        std::lock_guard<std::mutex> lock(mutex_);
        return state_==ZooKeeperConnectionState::Connected&&
               session_!=nullptr;
    }

    ZooKeeperConnectionEvent CurrentEvent()const noexcept{
        std::lock_guard<std::mutex> lock(mutex_);
        return {state_,generation_};
    }

    bool WaitUntilConnected(std::chrono::milliseconds timeout){
        if(timeout<std::chrono::milliseconds::zero()){
            throw std::invalid_argument(
                "ZooKeeper wait timeout must not be negative"
            );
        }

        std::unique_lock<std::mutex> lock(mutex_);
        connected_condition_.wait_for(lock,timeout,[this](){
            return (state_==ZooKeeperConnectionState::Connected&&
                    session_!=nullptr)||
                   state_==ZooKeeperConnectionState::AuthFailed||
                   !running_;
        });
        return state_==ZooKeeperConnectionState::Connected&&
               session_!=nullptr;
    }

    ListenerId AddStateListener(StateCallback callback){
        if(!callback){
            throw std::invalid_argument(
                "ZooKeeper state callback is empty"
            );
        }

        std::lock_guard<std::mutex> lock(mutex_);
        ListenerId id=next_listener_id_++;
        while(id==0){
            id=next_listener_id_++;
        }
        listeners_.emplace(id,std::move(callback));
        return id;
    }

    void RemoveStateListener(ListenerId id)noexcept{
        std::lock_guard<std::mutex> lock(mutex_);
        listeners_.erase(id);
    }

    void EnsurePersistentPath(const std::string& path){
        std::vector<std::string> paths=ParentPaths(path);
        std::shared_ptr<Session> session=ConnectedSession();

        for(const std::string& parent:paths){
            int result=zoo_create(
                session->handle.get(),
                parent.c_str(),
                nullptr,
                0,
                &ZOO_OPEN_ACL_UNSAFE,
                0,
                nullptr,
                0
            );

            if(result!=ZOK&&result!=ZNODEEXISTS){
                throw ZooKeeperError(
                    "create persistent node "+parent,
                    result
                );
            }
        }
    }

    std::string CreateEphemeralSequential(
        const std::string& path,
        const std::string& data
    ){
        ValidateNodePrefix(path);
        if(data.size()>static_cast<std::size_t>(INT_MAX)){
            throw std::invalid_argument(
                "ZooKeeper node data is too large"
            );
        }
        std::shared_ptr<Session> session=ConnectedSession();
        std::vector<char> created_path(4096);

        int result=zoo_create(
            session->handle.get(),
            path.c_str(),
            data.data(),
            static_cast<int>(data.size()),
            &ZOO_OPEN_ACL_UNSAFE,
            ZOO_EPHEMERAL|ZOO_SEQUENCE,
            created_path.data(),
            static_cast<int>(created_path.size())
        );

        if(result!=ZOK){
            throw ZooKeeperError(
                "create ephemeral sequential node "+path,
                result
            );
        }
        return std::string(created_path.data());
    }

    std::vector<std::string> GetChildrenAndWatch(
        const std::string& path,
        ChildrenWatchCallback callback
    ){
        ParentPaths(path);
        if(!callback){
            throw std::invalid_argument(
                "ZooKeeper children watch callback is empty"
            );
        }

        std::shared_ptr<Session> session=ConnectedSession();
        auto context=std::make_shared<WatchContext>();

        {
            std::lock_guard<std::mutex> lock(mutex_);
            if(session_!=session||
               state_!=ZooKeeperConnectionState::Connected){
                throw std::logic_error(
                    "ZooKeeper session changed before installing watch"
                );
            }

            context->bridge=bridge_;
            context->session=session;
            context->generation=generation_;
            context->id=NextWatchIdInLock();
            context->callback=std::move(callback);
            session->watches.emplace(context->id,context);
        }

        String_vector children{};
        int result=zoo_wget_children(
            session->handle.get(),
            path.c_str(),
            &Impl::ChildrenWatcher,
            context.get(),
            &children
        );

        if(result!=ZOK){
            RemoveWatch(session,context->id);
            if(children.data!=nullptr){
                deallocate_String_vector(&children);
            }
            throw ZooKeeperError(
                "get children and watch "+path,
                result
            );
        }

        std::vector<std::string> result_children;
        result_children.reserve(
            static_cast<std::size_t>(children.count)
        );
        for(int index=0;index<children.count;++index){
            result_children.emplace_back(children.data[index]);
        }
        deallocate_String_vector(&children);
        return result_children;
    }

    std::string GetData(const std::string& path){
        ParentPaths(path);
        std::shared_ptr<Session> session=ConnectedSession();

        for(int attempt=0;attempt<3;++attempt){
            Stat stat{};
            int result=zoo_exists(
                session->handle.get(),
                path.c_str(),
                0,
                &stat
            );
            if(result!=ZOK){
                throw ZooKeeperError("get node stat "+path,result);
            }

            if(stat.dataLength<0){
                return {};
            }

            std::vector<char> data(
                std::max<std::size_t>(
                    static_cast<std::size_t>(stat.dataLength),
                    1
                )
            );
            int data_length=static_cast<int>(data.size());
            result=zoo_get(
                session->handle.get(),
                path.c_str(),
                0,
                data.data(),
                &data_length,
                nullptr
            );

            if(result==ZOK){
                if(data_length<=0){
                    return {};
                }
                return std::string(
                    data.data(),
                    static_cast<std::size_t>(data_length)
                );
            }
            if(result!=ZMARSHALLINGERROR){
                throw ZooKeeperError("get node data "+path,result);
            }
        }

        throw std::runtime_error(
            "ZooKeeper node data changed too frequently: "+path
        );
    }

    std::string GetDataAndWatch(
        const std::string& path,
        DataWatchCallback callback
    ){
        ParentPaths(path);
        if(!callback){
            throw std::invalid_argument(
                "ZooKeeper data watch callback is empty"
            );
        }

        std::shared_ptr<Session> session=ConnectedSession();
        auto context=std::make_shared<WatchContext>();

        {
            std::lock_guard<std::mutex> lock(mutex_);
            if(session_!=session||
               state_!=ZooKeeperConnectionState::Connected){
                throw std::logic_error(
                    "ZooKeeper session changed before installing watch"
                );
            }

            context->bridge=bridge_;
            context->session=session;
            context->generation=generation_;
            context->id=NextWatchIdInLock();
            context->callback=std::move(callback);
            session->watches.emplace(context->id,context);
        }

        Stat stat{};
        int result=zoo_wexists(
            session->handle.get(),
            path.c_str(),
            &Impl::DataWatcher,
            context.get(),
            &stat
        );
        if(result!=ZOK){
            RemoveWatch(session,context->id);
            throw ZooKeeperError(
                "watch node data "+path,
                result
            );
        }

        try{
            return GetData(path);
        }catch(...){
            // zoo_wexists成功后Watch仍由ZooKeeper持有，context必须保留到
            // Watch触发或Session销毁，不能从session->watches中提前删除。
            throw;
        }
    }

    void SetData(
        const std::string& path,
        const std::string& data
    ){
        ParentPaths(path);
        if(data.size()>static_cast<std::size_t>(INT_MAX)){
            throw std::invalid_argument(
                "ZooKeeper node data is too large"
            );
        }

        std::shared_ptr<Session> session=ConnectedSession();
        int result=zoo_set(
            session->handle.get(),
            path.c_str(),
            data.data(),
            static_cast<int>(data.size()),
            -1
        );
        if(result!=ZOK){
            throw ZooKeeperError("set node data "+path,result);
        }
    }

private:
    struct WatcherBridge{
        std::mutex mutex;
        Impl* owner=nullptr;
    };

    struct SessionContext{
        std::shared_ptr<WatcherBridge> bridge;
        std::uint64_t generation=0;
    };

    struct Session;

    struct WatchContext:
        public std::enable_shared_from_this<WatchContext>{
        std::shared_ptr<WatcherBridge> bridge;
        std::weak_ptr<Session> session;
        std::uint64_t generation=0;
        std::uint64_t id=0;
        ChildrenWatchCallback callback;
    };

    struct HandleDeleter{
        void operator()(zhandle_t* handle)const noexcept{
            if(handle!=nullptr){
                zookeeper_close(handle);
            }
        }
    };

    struct Session{
        std::unique_ptr<SessionContext> context;
        std::map<std::uint64_t,std::shared_ptr<WatchContext>> watches;
        std::unique_ptr<zhandle_t,HandleDeleter> handle;
    };

    static void Watcher(
        zhandle_t*,
        int type,
        int state,
        const char*,
        void* context
    ){
        if(type!=ZOO_SESSION_EVENT||context==nullptr){
            return;
        }

        auto* session_context=
            static_cast<SessionContext*>(context);
        std::shared_ptr<WatcherBridge> bridge=
            session_context->bridge;

        std::lock_guard<std::mutex> lock(bridge->mutex);
        if(bridge->owner!=nullptr){
            bridge->owner->HandleWatcher(
                session_context->generation,
                state
            );
        }
    }

    static void ChildrenWatcher(
        zhandle_t*,
        int type,
        int,
        const char*,
        void* context
    ){
        if(context==nullptr||type==ZOO_SESSION_EVENT){
            return;
        }

        auto* raw_context=static_cast<WatchContext*>(context);
        std::shared_ptr<WatchContext> watch_context=
            raw_context->shared_from_this();
        std::shared_ptr<WatcherBridge> bridge=
            watch_context->bridge;

        std::lock_guard<std::mutex> lock(bridge->mutex);
        if(bridge->owner!=nullptr){
            bridge->owner->HandleWatch(
                std::move(watch_context)
            );
        }
    }

    static void DataWatcher(
        zhandle_t*,
        int type,
        int,
        const char*,
        void* context
    ){
        if(context==nullptr||type==ZOO_SESSION_EVENT){
            return;
        }

        auto* raw_context=static_cast<WatchContext*>(context);
        std::shared_ptr<WatchContext> watch_context=
            raw_context->shared_from_this();
        std::shared_ptr<WatcherBridge> bridge=
            watch_context->bridge;

        std::lock_guard<std::mutex> lock(bridge->mutex);
        if(bridge->owner!=nullptr){
            bridge->owner->HandleWatch(std::move(watch_context));
        }
    }

    void HandleWatch(
        std::shared_ptr<WatchContext> context
    ){
        std::lock_guard<std::mutex> lock(mutex_);
        std::shared_ptr<Session> session=context->session.lock();
        if(session!=nullptr){
            session->watches.erase(context->id);
        }

        if(!running_||
           context->generation!=generation_||
           session_!=session){
            return;
        }

        // ZooKeeper watcher在线程库内部执行，不能在这里调用同步API。
        tasks_.push_back(std::move(context->callback));
        condition_.notify_all();
    }

    void HandleWatcher(std::uint64_t generation,int state){
        std::lock_guard<std::mutex> lock(mutex_);
        if(!running_||generation!=generation_){
            return;
        }

        if(state==ZOO_CONNECTED_STATE){
            SetStateInLock(
                ZooKeeperConnectionState::Connected,
                generation
            );
            connected_condition_.notify_all();
        }else if(state==ZOO_EXPIRED_SESSION_STATE){
            SetStateInLock(
                ZooKeeperConnectionState::Expired,
                generation
            );
            reconnect_requested_=true;
            connected_condition_.notify_all();
        }else if(state==ZOO_AUTH_FAILED_STATE){
            SetStateInLock(
                ZooKeeperConnectionState::AuthFailed,
                generation
            );
            connected_condition_.notify_all();
        }else{
            SetStateInLock(
                ZooKeeperConnectionState::Disconnected,
                generation
            );
        }
        condition_.notify_all();
    }

    void SetStateInLock(
        ZooKeeperConnectionState state,
        std::uint64_t generation
    ){
        if(state_==state&&generation_==generation){
            return;
        }

        state_=state;
        events_.push_back({state,generation});
    }

    void WorkerLoop(){
        while(true){
            ZooKeeperConnectionEvent event;
            bool has_event=false;
            std::function<void()> task;
            bool reconnect=false;
            std::shared_ptr<Session> old_session;

            {
                std::unique_lock<std::mutex> lock(mutex_);
                condition_.wait(lock,[this](){
                    return !running_||
                           !events_.empty()||
                           !tasks_.empty()||
                           reconnect_requested_;
                });

                if(!running_){
                    old_session=std::move(session_);
                    lock.unlock();
                    old_session.reset();
                    return;
                }

                if(!events_.empty()){
                    event=events_.front();
                    events_.pop_front();
                    has_event=true;
                }else if(!tasks_.empty()){
                    task=std::move(tasks_.front());
                    tasks_.pop_front();
                }else if(reconnect_requested_){
                    reconnect_requested_=false;
                    old_session=std::move(session_);
                    reconnect=true;
                }
            }

            if(has_event){
                NotifyListeners(event);
                continue;
            }
            if(task){
                try{
                    task();
                }catch(...){
                }
                continue;
            }

            if(reconnect){
                old_session.reset();
                if(!CreateSession()){
                    std::unique_lock<std::mutex> lock(mutex_);
                    condition_.wait_for(
                        lock,
                        options_.reconnect_delay,
                        [this](){
                            return !running_||
                                   !events_.empty()||
                                   !tasks_.empty();
                        }
                    );
                    if(running_){
                        reconnect_requested_=true;
                    }
                }
            }
        }
    }

    bool CreateSession(){
        auto session=std::make_shared<Session>();

        {
            std::lock_guard<std::mutex> lock(mutex_);
            if(!running_){
                return true;
            }

            ++generation_;
            if(generation_==0){
                ++generation_;
            }
            SetStateInLock(
                ZooKeeperConnectionState::Connecting,
                generation_
            );

            session->context=std::make_unique<SessionContext>();
            session->context->bridge=bridge_;
            session->context->generation=generation_;
        }

        zhandle_t* handle=zookeeper_init(
            options_.servers.c_str(),
            &Impl::Watcher,
            static_cast<int>(options_.session_timeout.count()),
            nullptr,
            session->context.get(),
            0
        );

        if(handle==nullptr){
            std::lock_guard<std::mutex> lock(mutex_);
            SetStateInLock(
                ZooKeeperConnectionState::Disconnected,
                generation_
            );
            return false;
        }
        session->handle.reset(handle);

        {
            std::lock_guard<std::mutex> lock(mutex_);
            if(!running_||
               session->context->generation!=generation_){
                return true;
            }
            session_=std::move(session);
            connected_condition_.notify_all();
        }
        condition_.notify_all();
        return true;
    }

    void NotifyListeners(const ZooKeeperConnectionEvent& event){
        std::vector<StateCallback> callbacks;

        {
            std::lock_guard<std::mutex> lock(mutex_);
            callbacks.reserve(listeners_.size());
            for(const auto& listener:listeners_){
                callbacks.push_back(listener.second);
            }
        }

        for(const StateCallback& callback:callbacks){
            try{
                callback(event);
            }catch(...){
            }
        }
    }

    std::shared_ptr<Session> ConnectedSession()const{
        std::lock_guard<std::mutex> lock(mutex_);
        if(state_!=ZooKeeperConnectionState::Connected||
           session_==nullptr){
            throw std::logic_error("ZooKeeper client is not connected");
        }
        return session_;
    }

    std::uint64_t NextWatchIdInLock(){
        std::uint64_t id=next_watch_id_++;
        while(id==0){
            id=next_watch_id_++;
        }
        return id;
    }

    void RemoveWatch(
        const std::shared_ptr<Session>& session,
        std::uint64_t watch_id
    ){
        std::lock_guard<std::mutex> lock(mutex_);
        session->watches.erase(watch_id);
    }

    ZooKeeperClientOptions options_;
    std::shared_ptr<WatcherBridge> bridge_;

    mutable std::mutex mutex_;
    std::condition_variable condition_;
    std::condition_variable connected_condition_;
    bool running_=false;
    bool reconnect_requested_=false;
    ZooKeeperConnectionState state_=
        ZooKeeperConnectionState::Stopped;
    std::uint64_t generation_=0;
    ListenerId next_listener_id_=1;
    std::map<ListenerId,StateCallback> listeners_;
    std::deque<ZooKeeperConnectionEvent> events_;
    std::deque<std::function<void()>> tasks_;
    std::uint64_t next_watch_id_=1;
    std::shared_ptr<Session> session_;
    std::thread worker_;
};

ZooKeeperClient::ZooKeeperClient(ZooKeeperClientOptions options)
    :impl_(std::make_unique<Impl>(std::move(options))){}

ZooKeeperClient::~ZooKeeperClient()=default;

void ZooKeeperClient::Start(){
    impl_->Start();
}

void ZooKeeperClient::Close()noexcept{
    impl_->Close();
}

bool ZooKeeperClient::IsConnected()const noexcept{
    return impl_->IsConnected();
}

ZooKeeperConnectionEvent ZooKeeperClient::CurrentEvent()const noexcept{
    return impl_->CurrentEvent();
}

bool ZooKeeperClient::WaitUntilConnected(
    std::chrono::milliseconds timeout
){
    return impl_->WaitUntilConnected(timeout);
}

ZooKeeperClient::ListenerId ZooKeeperClient::AddStateListener(
    StateCallback callback
){
    return impl_->AddStateListener(std::move(callback));
}

void ZooKeeperClient::RemoveStateListener(
    ListenerId listener_id
)noexcept{
    impl_->RemoveStateListener(listener_id);
}

void ZooKeeperClient::EnsurePersistentPath(
    const std::string& path
){
    impl_->EnsurePersistentPath(path);
}

std::string ZooKeeperClient::CreateEphemeralSequential(
    const std::string& path_prefix,
    const std::string& data
){
    return impl_->CreateEphemeralSequential(path_prefix,data);
}

std::vector<std::string> ZooKeeperClient::GetChildrenAndWatch(
    const std::string& path,
    ChildrenWatchCallback callback
){
    return impl_->GetChildrenAndWatch(
        path,
        std::move(callback)
    );
}

std::string ZooKeeperClient::GetData(const std::string& path){
    return impl_->GetData(path);
}

std::string ZooKeeperClient::GetDataAndWatch(
    const std::string& path,
    DataWatchCallback callback
){
    return impl_->GetDataAndWatch(path,std::move(callback));
}

void ZooKeeperClient::SetData(
    const std::string& path,
    const std::string& data
){
    impl_->SetData(path,data);
}

}
