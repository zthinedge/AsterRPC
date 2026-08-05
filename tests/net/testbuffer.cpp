#include "asterrpc/net/Buffer.h"

#include <cassert>
#include <string>

using asterrpc::net::Buffer;

int main() {
    Buffer buffer(4);

    assert(buffer.ReadableBytes() == 0);
    assert(buffer.WritableBytes() == 4);

    buffer.Append("hello", 5);

    assert(buffer.ReadableBytes() == 5);
    assert(buffer.RetrieveAsString(2) == "he");
    assert(buffer.ReadableBytes() == 3);
    assert(buffer.RetrieveAllAsString() == "llo");
    assert(buffer.ReadableBytes() == 0);

    buffer.Append("abc", 3);
    buffer.Append("def", 3);

    assert(buffer.RetrieveAllAsString() == "abcdef");
}