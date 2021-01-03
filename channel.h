// Copyright (C) 2020 Andrei Avram
// https://github.com/andreiavrammsd/cpp-channel

#include <atomic>
#include <condition_variable>
#include <cstdlib>
#include <mutex>
#include <queue>

/**
 *  @brief Exception throw if trying to write on closed channel.
 */
class ClosedChannel : public std::runtime_error {
   public:
    explicit ClosedChannel(const char* msg) : std::runtime_error(msg) {}
};

/**
 *  @brief Thread-safe container for sharing data between threads.
 *
 *  Implements a blocking input iterator.
 *
 *  @tparam T The type of the elements.
 */
template <typename T>
class Channel {
   public:
    using value_type = T;
    using size_type = std::size_t;

    /**
     * Creates a new channel.
     *
     * @param capacity Number of elements the channel can store before blocking.
     */
    Channel(size_type capacity = 0);

    /**
     * Pushes an element into the channel by copy.
     *
     * @tparam Type The type of the elements.
     *
     * @throws ClosedChannel if channel is closed.
     */
    template <typename Type>
    friend void operator>>(const Type&, Channel<Type>&);

    /**
     * Allows pushing an element into the channel by move.
     *
     * @tparam Type The type of the elements.
     *
     * @throws ClosedChannel if channel is closed.
     */
    template <typename Type>
    friend void operator>>(Type&&, Channel<Type>&);

    /**
     * Pops an element from the channel.
     *
     * @tparam Type The type of the elements.
     */
    template <typename Type>
    friend void operator<<(Type&, Channel<Type>&);

    /**
     * Returns the number of elements in the channel.
     */
    size_type size() const noexcept;

    /**
     * Returns true if there are no elements in channel.
     */
    bool empty() const noexcept;

    /**
     * Closes the channel.
     */
    void close() noexcept;

    /**
     * Returns true if the channel is closed.
     */
    bool closed() const noexcept;

    /**
     * Channel cannot be copied or moved.
     */
    Channel(const Channel&) = delete;
    Channel& operator=(const Channel&) = delete;
    Channel(Channel&&) = delete;
    Channel& operator=(Channel&&) = delete;
    virtual ~Channel() = default;

   private:
    const size_type cap;
    std::queue<T> queue;
    std::mutex mtx;
    std::condition_variable cnd;
    std::atomic<bool> is_closed;
};

#include "channel.cc"

