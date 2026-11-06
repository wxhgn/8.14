#pragma once

#include <vector>
#include <string>
#include <algorithm>
#include <sys/uio.h>
#include <cstddef>

// muduo风格Buffer缓冲区
class Buffer
{
public:
    static const size_t kCheapPrepend = 8;
    static const size_t kInitialSize = 1024;

    explicit Buffer(size_t initialSize = kInitialSize)
        : buffer_(kCheapPrepend + initialSize),
          readerIndex_(kCheapPrepend),
          writerIndex_(kCheapPrepend)
    {}

    size_t readableBytes() const { return writerIndex_ - readerIndex_; }
    size_t writableBytes() const { return buffer_.size() - writerIndex_; }
    size_t prependableBytes() const { return readerIndex_; }

    const char* peek() const { return begin() + readerIndex_; }
    char* beginWrite() { return begin() + writerIndex_; }
    const char* beginWrite() const { return begin() + writerIndex_; }

    void append(const char* data, size_t len)
    {
        ensureWritable(len);
        std::copy(data, data + len, beginWrite());
        writerIndex_ += len;
    }

    void append(const std::string& s)
    {
        append(s.data(), s.size());
    }

    void append(const std::vector<char>& vec)
    {
        append(vec.data(), vec.size());
    }

    void retrieve(size_t len)
    {
        if (len < readableBytes())
        {
            readerIndex_ += len;
        }
        else
        {
            retrieveAll();
        }
    }

    void retrieveAll()
    {
        readerIndex_ = kCheapPrepend;
        writerIndex_ = kCheapPrepend;
    }

    std::string retrieveAllAsString()
    {
        std::string res(peek(), readableBytes());
        retrieveAll();
        return res;
    }

    std::string retrieveAsString(size_t len)
    {
        std::string res(peek(), len);
        retrieve(len);
        return res;
    }

    void ensureWritable(size_t len)
    {
        if (writableBytes() >= len) return;
        if (writableBytes() + prependableBytes() >= len + kCheapPrepend)
        {
            size_t readable = readableBytes();
            std::copy(begin() + readerIndex_,
                      begin() + writerIndex_,
                      begin() + kCheapPrepend);
            readerIndex_ = kCheapPrepend;
            writerIndex_ = kCheapPrepend + readable;
        }
        else
        {
            buffer_.resize(writerIndex_ + len);
        }
    }

    ssize_t readFd(int fd, int* savedErrno)
    {
        char extrabuf[65536];
        struct iovec vec[2];
        const size_t writable = writableBytes();
        vec[0].iov_base = beginWrite();
        vec[0].iov_len  = writable;
        vec[1].iov_base = extrabuf;
        vec[1].iov_len  = sizeof extrabuf;

        const int iovcnt = (writable < sizeof extrabuf) ? 2 : 1;
        ssize_t n = ::readv(fd, vec, iovcnt);
        if (n < 0)
        {
            *savedErrno = errno;
        }
        else if ((size_t)n <= writable)
        {
            writerIndex_ += n;
        }
        else
        {
            writerIndex_ = buffer_.size();
            append(extrabuf, n - writable);
        }
        return n;
    }

private:
    char* begin() { return &*buffer_.begin(); }
    const char* begin() const { return &*buffer_.begin(); }

    std::vector<char> buffer_;
    size_t readerIndex_;
    size_t writerIndex_;
};
