#include <brpc/channel.h>
#include <brpc/redis.h>
#include <bthread/bthread.h>
#include <bthread/types.h>
#include <butil/logging.h>

#include <condition_variable>
#include <functional>
#include <mutex>
#include <random>
#include <vector>

class ReadWriteLock {
public:
    ReadWriteLock() : readers(0), writers(0), writing(false) {}

    void lockRead() {
        std::unique_lock<std::mutex> lk(mutex_);
        while (writers > 0 || writing) {
            cond_.wait(lk);
        }
        readers++;
        LOG(INFO) << "read lock by " << bthread_self();
    }

    void unlockRead() {
        std::unique_lock<std::mutex> lk(mutex_);
        readers--;
        if (readers == 0) {
            cond_.notify_all();
        }
        LOG(INFO) << "un read lock by " << bthread_self();
    }

    void lockWrite() {
        std::unique_lock<std::mutex> lk(mutex_);
        writers++;
        while (readers > 0 || writing) {
            cond_.wait(lk);
        }
        writing = true;
        LOG(INFO) << "write lock by " << bthread_self();
    }

    void unlockWrite() {
        std::unique_lock<std::mutex> lk(mutex_);
        writers--;
        writing = false;
        cond_.notify_all();
        LOG(INFO) << "un write lock by " << bthread_self();
    }

private:
    std::mutex mutex_;
    std::condition_variable cond_;
    int readers;
    int writers;
    bool writing;
};

class ReadLockGuard {
public:
    explicit ReadLockGuard(ReadWriteLock* rlock) : rlock_(rlock) { rlock_->lockRead(); }
    ~ReadLockGuard() { rlock_->unlockRead(); }

private:
    ReadWriteLock* rlock_;
};

class WriteLockGuard {
public:
    explicit WriteLockGuard(ReadWriteLock* wlock) : wlock_(wlock) { wlock_->lockWrite(); }
    ~WriteLockGuard() { wlock_->unlockWrite(); }

private:
    ReadWriteLock* wlock_;
};

int data = 0;

ReadWriteLock rwlock;

int set_value(brpc::Channel* channel, const std::string& key, int value);
int get_value(brpc::Channel* channel, const std::string& key, int* value);

void* thread_func(void* arg);

void* thread_func(void* arg) {
    LOG(INFO) << "start: " << bthread_self() << std::endl;
    static std::random_device seed;
    static std::ranlux48 engine(seed());
    std::uniform_int_distribution<uint64_t> distrib(0, 100000);
    std::string key = "test";
    {
        WriteLockGuard guard(&rwlock);
        // do write
        data = distrib(engine);
        if (set_value(reinterpret_cast<brpc::Channel*>(arg), key, data) != 0) {
            LOG(ERROR) << "Fail to set value";
        }

        LOG(INFO) << "write to data: " << data;
    }
    {
        ReadLockGuard guard(&rwlock);
        // do read
        int value;
        if (get_value(reinterpret_cast<brpc::Channel*>(arg), key, &value) != 0) {
            LOG(ERROR) << "Fail to get value";
        }
        LOG(INFO) << "read from data: " << data << ", value:" << value;
    }
    LOG(INFO) << "end: " << bthread_self() << std::endl;
    return nullptr;
}

int set_value(brpc::Channel* channel, const std::string& key, int value) {
    brpc::RedisRequest request;
    brpc::RedisResponse response;
    brpc::Controller cntl;
    cntl.set_timeout_ms(1000);  // 设置超时时间为 1 秒钟
    request.AddCommand("SET %s %d", key, value);
    channel->CallMethod(nullptr, &cntl, &request, &response, nullptr);

    if (cntl.Failed()) {
        LOG(ERROR) << "Fail to access redis-server";
        return -1;
    }
    if (response.reply(0).is_error()) {
        LOG(ERROR) << "Fail to incr";
        return -1;
    }
    return 0;
}

int get_value(brpc::Channel* channel, const std::string& key, int* value) {
    brpc::RedisRequest request;
    brpc::RedisResponse response;
    brpc::Controller cntl;
    cntl.set_timeout_ms(1000);  // 设置超时时间为 1 秒钟
    request.AddCommand("GET %s", key);
    channel->CallMethod(nullptr, &cntl, &request, &response, nullptr);

    if (cntl.Failed()) {
        LOG(ERROR) << "Fail to access redis-server";
        return -1;
    }
    if (response.reply(0).is_error()) {
        LOG(ERROR) << "Fail to incr";
        return -1;
    }
    *value = response.reply(0).integer();
    return 0;
}

int main(int args, char* argv[]) {
    int thread_num = 16;
    brpc::ChannelOptions options;
    options.protocol = brpc::PROTOCOL_REDIS;
    brpc::Channel redis_channel;
    if (redis_channel.Init("127.0.0.1:7890", &options) != 0) {
        LOG(ERROR) << "Fail to init channel to redis-server";
        return -1;
    }
    auto bids = std::vector<bthread_t>(thread_num);
    for (int i = 0; i < thread_num; ++i) {
        if (bthread_start_background(&bids[i], nullptr, thread_func, &redis_channel) != 0) {
            LOG(ERROR) << "Fail to create bthread";
        }
    }
    for (int i = 0; i < thread_num; ++i) {
        bthread_join(bids[i], nullptr);
    }
    return 0;
}
