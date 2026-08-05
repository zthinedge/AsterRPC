#pragma once

#include "asterrpc/log/LogSink.h"

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <string_view>

namespace asterrpc::log{

class RollingFileSink:public LogSink{
public:
    RollingFileSink(
        std::filesystem::path file_path,
        std::size_t roll_size_bytes,
        std::size_t max_rolled_files
    );

    bool Write(std::string_view bytes)override;
    bool Flush()override;

private:
    bool OpenFile();
    bool RollFile();
    void RemoveOldFiles()noexcept;
    std::filesystem::path MakeRolledPath();
    bool IsRolledFile(const std::filesystem::path& path)const;

    std::filesystem::path file_path_;
    std::size_t roll_size_bytes_;
    std::size_t max_rolled_files_;
    std::size_t current_size_;
    std::uint64_t roll_sequence_;
    std::ofstream file_;
};

}
