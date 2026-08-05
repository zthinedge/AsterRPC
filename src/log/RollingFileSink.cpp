#include "RollingFileSink.h"

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <ctime>
#include <iomanip>
#include <sstream>
#include <stdexcept>
#include <system_error>
#include <unistd.h>
#include <vector>

namespace asterrpc::log{
namespace{

std::tm ToLocalTime(std::time_t time){
    std::tm result{};
    ::localtime_r(&time,&result);
    return result;
}

}

RollingFileSink::RollingFileSink(
    std::filesystem::path file_path,
    std::size_t roll_size_bytes,
    std::size_t max_rolled_files
):file_path_(std::move(file_path)),
  roll_size_bytes_(roll_size_bytes),
  max_rolled_files_(max_rolled_files),
  current_size_(0),
  roll_sequence_(0){
    if(file_path_.empty()){
        throw std::invalid_argument("log file path must not be empty");
    }

    std::filesystem::path parent=file_path_.parent_path();
    if(!parent.empty()){
        std::error_code error;
        std::filesystem::create_directories(parent,error);
        if(error){
            throw std::runtime_error(
                "failed to create log directory: "+error.message()
            );
        }
    }

    if(!OpenFile()){
        throw std::runtime_error(
            "failed to open log file: "+file_path_.string()
        );
    }
}

bool RollingFileSink::Write(std::string_view bytes){
    if(bytes.empty()){
        return true;
    }

    bool needs_roll=
        roll_size_bytes_>0&&
        current_size_>0&&
        bytes.size()>roll_size_bytes_-std::min(
            current_size_,roll_size_bytes_
        );

    if(needs_roll&&!RollFile()){
        return false;
    }

    file_.write(bytes.data(),static_cast<std::streamsize>(bytes.size()));
    if(!file_){
        return false;
    }

    current_size_+=bytes.size();
    return true;
}

bool RollingFileSink::Flush(){
    file_.flush();
    return static_cast<bool>(file_);
}

bool RollingFileSink::OpenFile(){
    file_.clear();
    file_.open(file_path_,std::ios::binary|std::ios::app);
    if(!file_){
        return false;
    }

    std::error_code error;
    std::uintmax_t size=std::filesystem::file_size(file_path_,error);
    current_size_=error?0:static_cast<std::size_t>(size);
    return true;
}

bool RollingFileSink::RollFile(){
    file_.flush();
    file_.close();

    std::filesystem::path rolled_path=MakeRolledPath();
    std::error_code error;
    std::filesystem::rename(file_path_,rolled_path,error);

    if(error){
        OpenFile();
        return false;
    }

    if(!OpenFile()){
        return false;
    }

    RemoveOldFiles();
    return true;
}

void RollingFileSink::RemoveOldFiles()noexcept{
    if(max_rolled_files_==0){
        return;
    }

    std::filesystem::path directory=file_path_.parent_path();
    if(directory.empty()){
        directory=".";
    }

    std::error_code error;
    std::filesystem::directory_iterator iterator(directory,error);
    std::vector<std::filesystem::path> files;

    while(!error&&iterator!=std::filesystem::directory_iterator()){
        if(iterator->is_regular_file(error)&&
           !error&&
           IsRolledFile(iterator->path())){
            files.push_back(iterator->path());
        }

        iterator.increment(error);
    }

    if(error||files.size()<=max_rolled_files_){
        return;
    }

    std::sort(files.begin(),files.end());
    std::size_t remove_count=files.size()-max_rolled_files_;

    for(std::size_t index=0;index<remove_count;++index){
        std::filesystem::remove(files[index],error);
        error.clear();
    }
}

std::filesystem::path RollingFileSink::MakeRolledPath(){
    auto now=std::chrono::system_clock::now();
    auto micros=std::chrono::duration_cast<std::chrono::microseconds>(
        now.time_since_epoch()
    ).count()%1000000;
    std::time_t time=std::chrono::system_clock::to_time_t(now);
    std::tm local_time=ToLocalTime(time);

    std::ostringstream name;
    name<<file_path_.stem().string()<<'.'
        <<std::put_time(&local_time,"%Y%m%d-%H%M%S")<<'-'
        <<std::setw(6)<<std::setfill('0')<<micros<<'-'
        <<::getpid()<<'-'<<roll_sequence_++
        <<file_path_.extension().string();

    return file_path_.parent_path()/name.str();
}

bool RollingFileSink::IsRolledFile(
    const std::filesystem::path& path
)const{
    std::string filename=path.filename().string();
    std::string prefix=file_path_.stem().string()+'.';

    return path!=file_path_&&
           filename.compare(0,prefix.size(),prefix)==0&&
           path.extension()==file_path_.extension();
}

}
