#include "asterrpc/cluster/ConnectionPool.h"

#include "asterrpc/net/EventLoop.h"
#include "asterrpc/net/InetAddress.h"
#include "asterrpc/rpc/RpcClient.h"
#include "asterrpc/trace/TraceContext.h"
#include "rpc/detail/CallSupport.h"

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <optional>
#include <stdexcept>
#include <utility>
#include <vector>

namespace asterrpc::cluster{

bool ConnectionPoolOptions::operator==(
    const ConnectionPoolOptions& other
)const noexcept{
    return max_connections==other.max_connections&&
           idle_timeout==other.idle_timeout&&
           reap_interval==other.reap_interval;
}

bool ConnectionPoolOptions::operator!=(
    const ConnectionPoolOptions& other
)const noexcept{
    return !(*this==other);
}

class ConnectionPool::Impl:
    public std::enable_shared_from_this<ConnectionPool::Impl>{
public:
    Impl(
        net::EventLoop* loop,
        Endpoint endpoint,
        ConnectionPoolOptions options
    ):loop_(loop),
      endpoint_(std::move(endpoint)),
      options_(options),
      next_entry_id_(1),
      reaper_timer_(0),
      stopping_(false),
      connection_count_(0),
      connected_count_(0),
      in_flight_count_(0),
      retry_count_(0){
        if(loop_==nullptr){
            throw std::invalid_argument(
                "connection pool EventLoop is null"
            );
        }
        if(options_.max_connections==0){
            throw std::invalid_argument(
                "connection pool max connections must be positive"
            );
        }
        if(options_.idle_timeout<
               std::chrono::milliseconds::zero()||
           options_.reap_interval<=
               std::chrono::milliseconds::zero()){
            throw std::invalid_argument(
                "connection pool timeouts are invalid"
            );
        }
    }

    void Start(){
        if(options_.idle_timeout>
           std::chrono::milliseconds::zero()){
            ScheduleReaper();
        }
    }

    void Shutdown(){
        auto self=shared_from_this();
        loop_->RunInLoop([self](){
            self->ShutdownInLoop();
        });
    }

    void Submit(
        std::string service_name,
        std::string method_name,
        std::string payload,
        rpc::CallOptions call_options,
        RetryPolicy retry_policy,
        ResponseCallback completion
    ){
        if(!completion){
            throw std::invalid_argument(
                "connection pool response callback is empty"
            );
        }

        auto state=std::make_shared<CallState>();
        state->service_name=std::move(service_name);
        state->method_name=std::move(method_name);
        state->payload=std::move(payload);
        state->call_options=call_options;
        state->call_options.deadline_us=
            rpc::detail::ResolveDeadline(call_options);
        state->call_options.timeout=std::chrono::microseconds::zero();
        state->call_options.max_retries=0;
        state->retry_policy=std::move(retry_policy);
        state->completion=std::move(completion);
        const trace::TraceContext* current=
            trace::CurrentTraceContext();
        if(current!=nullptr){
            state->parent_trace=*current;
            if(current->deadline_us!=0&&
               (state->call_options.deadline_us==0||
                current->deadline_us<
                    state->call_options.deadline_us)){
                state->call_options.deadline_us=
                    current->deadline_us;
            }
        }
        state->started_at=metrics::RpcMetrics::Clock::now();
        metrics_.RequestStarted(
            state->service_name,
            state->method_name
        );

        auto self=shared_from_this();
        loop_->RunInLoop([self,state](){
            self->ArmDeadline(state);
            self->StartAttempt(state);
        });
    }

    const Endpoint& GetEndpoint()const noexcept{
        return endpoint_;
    }

    const ConnectionPoolOptions& Options()const noexcept{
        return options_;
    }

    ConnectionPoolStats GetStats()const noexcept{
        ConnectionPoolStats stats;
        stats.connections=
            connection_count_.load(std::memory_order_relaxed);
        stats.connected=
            connected_count_.load(std::memory_order_relaxed);
        stats.in_flight=
            in_flight_count_.load(std::memory_order_relaxed);
        stats.retries=retry_count_.load(std::memory_order_relaxed);
        return stats;
    }

    metrics::RpcMetricsSnapshot GetMetrics()const noexcept{
        return metrics_.Snapshot();
    }

    metrics::RpcMetricsSnapshot GetMethodMetrics(
        std::string_view service_name,
        std::string_view method_name
    )const noexcept{
        return metrics_.MethodSnapshot(service_name,method_name);
    }

    std::vector<metrics::RpcMethodMetricsSnapshot>
    GetAllMethodMetrics()const{
        return metrics_.MethodSnapshots();
    }

    bool IsInLoopThread()const noexcept{
        return loop_->IsInLoopThread();
    }

private:
    enum class EntryState{
        Connecting,
        Connected,
        Failed,
        Closing
    };

    struct CallState{
        std::string service_name;
        std::string method_name;
        std::string payload;
        rpc::CallOptions call_options;
        RetryPolicy retry_policy;
        ResponseCallback completion;
        std::optional<trace::TraceContext> parent_trace;
        std::size_t attempts=0;
        net::EventLoop::TimerId deadline_timer=0;
        metrics::RpcMetrics::TimePoint started_at;
        bool finished=false;
    };

    struct Entry{
        std::uint64_t id=0;
        EntryState state=EntryState::Connecting;
        std::unique_ptr<rpc::RpcClient> client;
        std::size_t in_flight=0;
        std::vector<std::shared_ptr<CallState>> waiting_calls;
        net::EventLoop::TimePoint last_used=
            net::EventLoop::Clock::now();
    };

    void ArmDeadline(const std::shared_ptr<CallState>& state){
        std::uint64_t deadline=state->call_options.deadline_us;
        if(deadline==0){
            return;
        }

        std::uint64_t now=rpc::detail::CurrentTimeMicros();
        std::uint64_t remaining=deadline>now?deadline-now:0;
        auto self=shared_from_this();
        state->deadline_timer=loop_->RunAfter(
            std::chrono::microseconds(remaining),
            [self,state](){
                self->Complete(
                    state,
                    rpc::detail::MakeErrorResponse(
                        protocol::StatusCode::Timeout,
                        "rpc deadline exceeded while waiting for connection"
                    )
                );
            }
        );
    }

    void StartAttempt(const std::shared_ptr<CallState>& state){
        if(state->finished){
            return;
        }
        if(stopping_){
            Complete(
                state,
                rpc::detail::MakeErrorResponse(
                    protocol::StatusCode::ConnectionFailed,
                    "connection pool is stopping"
                )
            );
            return;
        }
        if(rpc::detail::DeadlineReached(
               state->call_options.deadline_us
           )){
            Complete(
                state,
                rpc::detail::MakeErrorResponse(
                    protocol::StatusCode::Timeout,
                    "rpc deadline exceeded"
                )
            );
            return;
        }

        ++state->attempts;
        Entry* connected=FindLeastLoadedConnection();
        std::size_t active=ActiveConnectionCount();

        if(connected!=nullptr&&connected->in_flight==0){
            Dispatch(connected,state);
            return;
        }

        if(active<options_.max_connections){
            Entry* entry=CreateConnection();
            if(entry!=nullptr){
                entry->waiting_calls.push_back(state);
                return;
            }
        }

        if(connected!=nullptr){
            Dispatch(connected,state);
            return;
        }

        Entry* connecting=FindLeastQueuedConnection();
        if(connecting!=nullptr){
            connecting->waiting_calls.push_back(state);
            return;
        }

        HandleAttemptFailure(
            state,
            protocol::StatusCode::ConnectionFailed,
            "no available connection for "+endpoint_.ToString()
        );
    }

    Entry* CreateConnection(){
        std::uint64_t id=next_entry_id_++;

        try{
            auto entry=std::make_unique<Entry>();
            entry->id=id;
            entry->client=std::make_unique<rpc::RpcClient>(
                loop_,
                net::InetAddress(endpoint_.Ip(),endpoint_.Port())
            );

            std::weak_ptr<Impl> weak_self=shared_from_this();
            entry->client->SetConnectionCallback([weak_self,id](){
                if(auto self=weak_self.lock()){
                    self->HandleConnected(id);
                }
            });
            entry->client->SetCloseCallback([weak_self,id](){
                if(auto self=weak_self.lock()){
                    self->HandleConnectionFailure(id,ECONNRESET);
                }
            });
            entry->client->SetErrorCallback([weak_self,id](int error){
                if(auto self=weak_self.lock()){
                    self->HandleConnectionFailure(id,error);
                }
            });

            Entry* result=entry.get();
            entries_.push_back(std::move(entry));
            connection_count_.fetch_add(1,std::memory_order_relaxed);
            result->client->Connect();
            return result;
        }catch(const std::exception&){
            return nullptr;
        }
    }

    void HandleConnected(std::uint64_t id){
        Entry* entry=FindEntry(id);
        if(entry==nullptr||entry->state!=EntryState::Connecting){
            return;
        }

        entry->state=EntryState::Connected;
        entry->last_used=net::EventLoop::Clock::now();
        connected_count_.fetch_add(1,std::memory_order_relaxed);
        metrics_.ConnectionOpened();

        std::vector<std::shared_ptr<CallState>> waiting;
        waiting.swap(entry->waiting_calls);
        for(const auto& state:waiting){
            if(!state->finished){
                Dispatch(entry,state);
            }
        }
    }

    void HandleConnectionFailure(std::uint64_t id,int error){
        Entry* entry=FindEntry(id);
        if(entry==nullptr||entry->state==EntryState::Failed){
            return;
        }

        if(entry->state==EntryState::Connected){
            connected_count_.fetch_sub(1,std::memory_order_relaxed);
            metrics_.ConnectionClosed();
        }
        entry->state=EntryState::Failed;

        std::vector<std::shared_ptr<CallState>> waiting;
        waiting.swap(entry->waiting_calls);
        std::string message=
            "connect to "+endpoint_.ToString()+" failed: "+
            std::strerror(error);

        for(const auto& state:waiting){
            HandleAttemptFailure(
                state,
                protocol::StatusCode::ConnectionFailed,
                message
            );
        }

        ScheduleErase(id);
    }

    void Dispatch(
        Entry* entry,
        const std::shared_ptr<CallState>& state
    ){
        if(entry==nullptr||
           entry->state!=EntryState::Connected||
           !entry->client->IsConnected()){
            HandleAttemptFailure(
                state,
                protocol::StatusCode::ConnectionFailed,
                "connection to "+endpoint_.ToString()+" is unavailable"
            );
            return;
        }

        ++entry->in_flight;
        entry->last_used=net::EventLoop::Clock::now();
        in_flight_count_.fetch_add(1,std::memory_order_relaxed);
        std::uint64_t entry_id=entry->id;
        auto self=shared_from_this();

        try{
            auto call=[&](){
                entry->client->AsyncCall(
                    state->service_name,
                    state->method_name,
                    state->payload,
                    [self,entry_id,state](
                        protocol::RpcMessage response
                    ){
                        self->HandleResponse(
                            entry_id,state,std::move(response)
                        );
                    },
                    state->call_options
                );
            };

            if(state->parent_trace){
                trace::TraceScope scope(*state->parent_trace);
                call();
            }else{
                call();
            }
        }catch(const std::exception& error){
            --entry->in_flight;
            in_flight_count_.fetch_sub(1,std::memory_order_relaxed);
            HandleAttemptFailure(
                state,
                protocol::StatusCode::InternalError,
                error.what()
            );
        }
    }

    void HandleResponse(
        std::uint64_t entry_id,
        const std::shared_ptr<CallState>& state,
        protocol::RpcMessage response
    ){
        Entry* entry=FindEntry(entry_id);
        if(entry!=nullptr){
            if(entry->in_flight>0){
                --entry->in_flight;
                in_flight_count_.fetch_sub(1,std::memory_order_relaxed);
            }
            entry->last_used=net::EventLoop::Clock::now();

            if(response.meta.status_code==
                   protocol::StatusCode::ConnectionFailed&&
               !entry->client->IsConnected()&&
               entry->state==EntryState::Connected){
                entry->state=EntryState::Failed;
                connected_count_.fetch_sub(1,std::memory_order_relaxed);
                metrics_.ConnectionClosed();
                ScheduleErase(entry_id);
            }
        }

        if(CanRetry(state,response.meta.status_code)&&
           !rpc::detail::DeadlineReached(
               state->call_options.deadline_us
           )){
            ScheduleRetry(state);
            return;
        }

        Complete(state,std::move(response));
    }

    void HandleAttemptFailure(
        const std::shared_ptr<CallState>& state,
        protocol::StatusCode status,
        std::string message
    ){
        if(CanRetry(state,status)&&
           !rpc::detail::DeadlineReached(
               state->call_options.deadline_us
           )){
            ScheduleRetry(state);
            return;
        }

        Complete(
            state,
            rpc::detail::MakeErrorResponse(status,std::move(message))
        );
    }

    void ScheduleRetry(const std::shared_ptr<CallState>& state){
        auto delay=state->retry_policy.BackoffForRetry(
            state->attempts
        );

        if(state->call_options.deadline_us!=0){
            std::uint64_t now=rpc::detail::CurrentTimeMicros();
            std::uint64_t remaining=
                state->call_options.deadline_us>now?
                state->call_options.deadline_us-now:0;
            if(static_cast<std::uint64_t>(delay.count())>=remaining){
                Complete(
                    state,
                    rpc::detail::MakeErrorResponse(
                        protocol::StatusCode::Timeout,
                        "retry backoff would exceed rpc deadline"
                    )
                );
                return;
            }
        }

        retry_count_.fetch_add(1,std::memory_order_relaxed);
        metrics_.RetryStarted(
            state->service_name,
            state->method_name
        );

        auto self=shared_from_this();
        if(delay==std::chrono::microseconds::zero()){
            loop_->QueueInLoop([self,state](){
                self->StartAttempt(state);
            });
            return;
        }

        loop_->RunAfter(delay,[self,state](){
            self->StartAttempt(state);
        });
    }

    void Complete(
        const std::shared_ptr<CallState>& state,
        protocol::RpcMessage response
    ){
        if(state->finished){
            return;
        }

        state->finished=true;
        if(state->deadline_timer!=0){
            loop_->CancelTimer(state->deadline_timer);
            state->deadline_timer=0;
        }
        metrics_.RequestFinished(
            state->service_name,
            state->method_name,
            response.meta.status_code,
            state->started_at
        );
        state->completion(std::move(response));
    }

    Entry* FindLeastLoadedConnection(){
        Entry* result=nullptr;
        for(const auto& entry:entries_){
            if(entry->state!=EntryState::Connected||
               !entry->client->IsConnected()){
                continue;
            }
            if(result==nullptr||entry->in_flight<result->in_flight){
                result=entry.get();
            }
        }
        return result;
    }

    bool CanRetry(
        const std::shared_ptr<CallState>& state,
        protocol::StatusCode status
    )const noexcept{
        return state->call_options.idempotent&&
               state->retry_policy.ShouldRetry(status,state->attempts);
    }

    Entry* FindLeastQueuedConnection(){
        Entry* result=nullptr;
        for(const auto& entry:entries_){
            if(entry->state!=EntryState::Connecting){
                continue;
            }
            if(result==nullptr||
               entry->waiting_calls.size()<
                   result->waiting_calls.size()){
                result=entry.get();
            }
        }
        return result;
    }

    Entry* FindEntry(std::uint64_t id){
        auto entry=std::find_if(
            entries_.begin(),entries_.end(),
            [id](const std::unique_ptr<Entry>& value){
                return value->id==id;
            }
        );
        return entry==entries_.end()?nullptr:entry->get();
    }

    std::size_t ActiveConnectionCount()const{
        std::size_t count=0;
        for(const auto& entry:entries_){
            if(entry->state==EntryState::Connecting||
               entry->state==EntryState::Connected){
                ++count;
            }
        }
        return count;
    }

    void ScheduleErase(std::uint64_t id){
        auto self=shared_from_this();
        loop_->QueueInLoop([self,id](){
            self->EraseEntry(id);
        });
    }

    void EraseEntry(std::uint64_t id){
        auto entry=std::find_if(
            entries_.begin(),entries_.end(),
            [id](const std::unique_ptr<Entry>& value){
                return value->id==id;
            }
        );
        if(entry==entries_.end()){
            return;
        }

        if((*entry)->state==EntryState::Connected){
            connected_count_.fetch_sub(1,std::memory_order_relaxed);
            metrics_.ConnectionClosed();
        }
        if((*entry)->in_flight>0){
            in_flight_count_.fetch_sub(
                (*entry)->in_flight,std::memory_order_relaxed
            );
        }
        entries_.erase(entry);
        connection_count_.fetch_sub(1,std::memory_order_relaxed);
    }

    void ScheduleReaper(){
        std::weak_ptr<Impl> weak_self=shared_from_this();
        reaper_timer_=loop_->RunAfter(
            options_.reap_interval,
            [weak_self](){
                if(auto self=weak_self.lock()){
                    self->ReapIdleConnections();
                }
            }
        );
    }

    void ReapIdleConnections(){
        if(stopping_){
            return;
        }

        auto now=net::EventLoop::Clock::now();
        for(const auto& entry:entries_){
            if(entry->state==EntryState::Connected&&
               entry->in_flight==0&&
               entry->waiting_calls.empty()&&
               now-entry->last_used>=options_.idle_timeout){
                entry->state=EntryState::Closing;
                connected_count_.fetch_sub(1,std::memory_order_relaxed);
                metrics_.ConnectionClosed();
                entry->client->Disconnect();
            }
        }
        ScheduleReaper();
    }

    void ShutdownInLoop(){
        if(stopping_){
            return;
        }
        stopping_=true;
        if(reaper_timer_!=0){
            loop_->CancelTimer(reaper_timer_);
            reaper_timer_=0;
        }

        for(const auto& entry:entries_){
            if(entry->state==EntryState::Connected){
                metrics_.ConnectionClosed();
            }
        }
        entries_.clear();
        connection_count_.store(0,std::memory_order_relaxed);
        connected_count_.store(0,std::memory_order_relaxed);
        in_flight_count_.store(0,std::memory_order_relaxed);
    }

    net::EventLoop* loop_;
    Endpoint endpoint_;
    ConnectionPoolOptions options_;
    std::uint64_t next_entry_id_;
    net::EventLoop::TimerId reaper_timer_;
    bool stopping_;
    std::vector<std::unique_ptr<Entry>> entries_;

    std::atomic_size_t connection_count_;
    std::atomic_size_t connected_count_;
    std::atomic_size_t in_flight_count_;
    std::atomic_size_t retry_count_;
    metrics::RpcMetrics metrics_;
};

ConnectionPool::ConnectionPool(
    net::EventLoop* loop,
    Endpoint endpoint,
    ConnectionPoolOptions options
):impl_(std::make_shared<Impl>(loop,std::move(endpoint),options)){
    impl_->Start();
}

ConnectionPool::~ConnectionPool(){
    impl_->Shutdown();
}

protocol::RpcMessage ConnectionPool::Call(
    std::string service_name,
    std::string method_name,
    std::string payload,
    rpc::CallOptions call_options,
    RetryPolicy retry_policy
){
    if(impl_->IsInLoopThread()){
        throw std::logic_error(
            "synchronous pooled rpc call in EventLoop thread"
        );
    }

    return FutureCall(
        std::move(service_name),
        std::move(method_name),
        std::move(payload),
        call_options,
        std::move(retry_policy)
    ).get();
}

ConnectionPool::ResponseFuture ConnectionPool::FutureCall(
    std::string service_name,
    std::string method_name,
    std::string payload,
    rpc::CallOptions call_options,
    RetryPolicy retry_policy
){
    auto promise=
        std::make_shared<std::promise<protocol::RpcMessage>>();
    ResponseFuture future=promise->get_future();

    AsyncCall(
        std::move(service_name),
        std::move(method_name),
        std::move(payload),
        [promise](protocol::RpcMessage response){
            promise->set_value(std::move(response));
        },
        call_options,
        std::move(retry_policy)
    );
    return future;
}

void ConnectionPool::AsyncCall(
    std::string service_name,
    std::string method_name,
    std::string payload,
    ResponseCallback callback,
    rpc::CallOptions call_options,
    RetryPolicy retry_policy
){
    impl_->Submit(
        std::move(service_name),
        std::move(method_name),
        std::move(payload),
        call_options,
        std::move(retry_policy),
        std::move(callback)
    );
}

const Endpoint& ConnectionPool::GetEndpoint()const noexcept{
    return impl_->GetEndpoint();
}

const ConnectionPoolOptions& ConnectionPool::Options()const noexcept{
    return impl_->Options();
}

ConnectionPoolStats ConnectionPool::GetStats()const noexcept{
    return impl_->GetStats();
}

metrics::RpcMetricsSnapshot ConnectionPool::GetMetrics()const noexcept{
    return impl_->GetMetrics();
}

metrics::RpcMetricsSnapshot ConnectionPool::GetMethodMetrics(
    std::string_view service_name,
    std::string_view method_name
)const noexcept{
    return impl_->GetMethodMetrics(service_name,method_name);
}

std::vector<metrics::RpcMethodMetricsSnapshot>
ConnectionPool::GetAllMethodMetrics()const{
    return impl_->GetAllMethodMetrics();
}

}
