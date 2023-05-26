#ifndef INCLUDE_MODERNDBS_BUFFER_MANAGER_H
#define INCLUDE_MODERNDBS_BUFFER_MANAGER_H

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <deque>
#include <exception>
#include <iostream>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <vector>

namespace moderndbs {

class BufferFrame {
  private:
    friend class BufferManager;

    // state of the current frame state
    enum State { CLEAN, DIRTY, NEW };

    // positon of the current frame
    enum Position { NONE, FIFO, LRU };

    // the page id
    uint64_t page_id;

    // how many threads are using the frame
    size_t thread_cnt = 0;

    size_t start_pos = 0;

    // a read/write lock to protect the page
    std::shared_timed_mutex frame_latch;

    // state of the buffer frame
    State state = NEW;

    Position position = NONE;

    // the actual data contained on the page
    char* data;

  public:
    /// Returns a pointer to this page's data.
    char* get_data();

    BufferFrame();
    BufferFrame(uint64_t page_id, char* data);
};

class buffer_full_error : public std::exception {
  public:
    [[nodiscard]] const char* what() const noexcept override {
        return "buffer is full";
    }
};

class BufferManager {
  private:
    // TODO: add your implementation here

    // hash table for all buffer frames
    std::unordered_map<uint64_t, BufferFrame> bufferframes;
    std::unique_ptr<char[]> buffer;

    const size_t page_size, page_count;
    size_t free_pos = 0;

    std::mutex manager_latch;
    std::mutex fifo_latch;
    std::mutex lru_latch;

    // FIFO queue for 2Q strategy
    std::deque<uint64_t> fifo;

    // LRU queue for 2Q strategy
    std::deque<uint64_t> lru;

  public:
    BufferManager(const BufferManager&) = delete;
    BufferManager(BufferManager&&) = delete;
    BufferManager& operator=(const BufferManager&) = delete;
    BufferManager& operator=(BufferManager&&) = delete;
    /// Constructor.
    /// @param[in] page_size  Size in bytes that all pages will have.
    /// @param[in] page_count Maximum number of pages that should reside in
    //                        memory at the same time.
    BufferManager(size_t page_size, size_t page_count);

    /// Destructor. Writes all dirty pages to disk.
    ~BufferManager();

    /// Returns a reference to a `BufferFrame` object for a given page id. When
    /// the page is not loaded into memory, it is read from disk. Otherwise the
    /// loaded page is used.
    /// When the page cannot be loaded because the buffer is full, throws the
    /// exception `buffer_full_error`.
    /// Is thread-safe w.r.t. other concurrent calls to `fix_page()` and
    /// `unfix_page()`.
    /// @param[in] exclusive If `exclusive` is true, the page is locked
    ///                      exclusively. Otherwise it is locked
    ///                      non-exclusively (shared).
    BufferFrame& fix_page(uint64_t page_id, bool exclusive);

    /// Takes a `BufferFrame` reference that was returned by an earlier call to
    /// `fix_page()` and unfixes it. When `is_dirty` is / true, the page is
    /// written back to disk eventually.
    void unfix_page(BufferFrame& page, bool is_dirty);

    /// Returns the page ids of all pages (fixed and unfixed) that are in the
    /// FIFO list in FIFO order.
    /// Is not thread-safe.
    [[nodiscard]] std::vector<uint64_t> get_fifo_list() const;

    /// Returns the page ids of all pages (fixed and unfixed) that are in the
    /// LRU list in LRU order.
    /// Is not thread-safe.
    [[nodiscard]] std::vector<uint64_t> get_lru_list() const;

    /// Returns the segment id for a given page id which is contained in the 16
    /// most significant bits of the page id.
    static constexpr uint16_t get_segment_id(uint64_t page_id) {
        return page_id >> 48;
    }

    /// Returns the page id within its segment for a given page id. This
    /// corresponds to the 48 least significant bits of the page id.
    static constexpr uint64_t get_segment_page_id(uint64_t page_id) {
        return page_id & ((1ull << 48) - 1);
    }

    /// @brief read page from disk into memory
    /// @param page_id
    void read_frame(uint64_t page_id,
                    std::unique_lock<std::mutex>& manager_lock);

    /// @brief lock a frame
    /// @param page_id
    /// @param exclusive if true, lock it exclusivly; false lock it shared
    void lock_frame(uint64_t page_id, bool exclusive);

    /// @brief unlock a frame
    /// @param page_id
    void unlock_frame(uint64_t page_id);

    /// @brief check whether there is page can be evicted from buffer frames
    /// @return true if one page can be evicted
    BufferFrame* evict_check();

    void write_back_to_disk(BufferFrame* frame,
                            std::unique_lock<std::mutex>& manager_lock);

    /// @brief evict a page from buffer frames
    void evict(BufferFrame* frame, std::unique_lock<std::mutex>& manager_lock);
};

} // namespace moderndbs

#endif
