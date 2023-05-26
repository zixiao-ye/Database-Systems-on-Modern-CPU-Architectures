#include "moderndbs/buffer_manager.h"
#include "moderndbs/file.h"

namespace moderndbs {

char* BufferFrame::get_data() { return data; }

BufferFrame::BufferFrame(){};
BufferFrame::BufferFrame(uint64_t page_id, char* data)
    : page_id(page_id), data(data) {}

BufferManager::BufferManager(size_t page_size, size_t page_count)
    : page_size(page_size), page_count(page_count),
      buffer(std::make_unique<char[]>(page_count * page_size)) {}

BufferManager::~BufferManager() {
    std::unique_lock<std::mutex> manager_lock(manager_latch);
    for (auto& frame : bufferframes) {
        write_back_to_disk(&frame.second, manager_lock);
    }
}

BufferFrame& BufferManager::fix_page(uint64_t page_id, bool exclusive) {
    // lock the whole buffet manager, when a thread is calling fix, others
    // cannot change it
    // std::lock_guard<std::mutex> manager_lock(manager_latch);
    // manager_latch.lock();
    std::unique_lock<std::mutex> manager_lock(manager_latch);
    // first check if the page is in lru, if found return the frame
    auto page_pos = std::find(lru.begin(), lru.end(), page_id);
    if (page_pos != lru.end()) {
        std::lock_guard<std::mutex> lru_lock(lru_latch);
        bufferframes[page_id].thread_cnt++;
        // If the page already in LRU, update it to the end of LRU
        lru.erase(page_pos);
        lru.push_back(page_id);
        manager_lock.unlock();
        return bufferframes[page_id];
    } else {
        // second check if the page is in fifo, if found return the frame
        auto pos = std::find(fifo.begin(), fifo.end(), page_id);
        if (pos != fifo.end()) {
            std::lock_guard<std::mutex> fifo_guard(fifo_latch);
            std::lock_guard<std::mutex> lru_guard(lru_latch);
            bufferframes[page_id].thread_cnt++;
            lru.push_back(page_id);
            fifo.erase(pos);
            bufferframes[page_id].position = BufferFrame::LRU;
            manager_lock.unlock();
            return bufferframes[page_id];
        } else {
            // try to find a free slot in buffer
            if (bufferframes.size() < page_count) {
                // lock frame in exclusive mode
                lock_frame(page_id, true);
                // read frame from disk using frmae's meta data
                read_frame(page_id, manager_lock);
                // add frame to fifo queue
                {
                    std::lock_guard<std::mutex> fifo_guard(fifo_latch);
                    bufferframes[page_id].position = BufferFrame::FIFO;
                    fifo.push_back(page_id);
                }
                // unlock frame in exclusive mode
                unlock_frame(page_id);
                // lock frame in user's requested mode return the frame
                lock_frame(page_id, exclusive);
                manager_lock.unlock();
                return bufferframes[page_id];
            } else {
                // bufferframes is full, try to evict one from fifo or lru
                auto free_frame = evict_check();
                if (free_frame) {
                    // can evict a frame from fifo, lru
                    // evict the frame nobody use
                    auto free_frame_pos = free_frame->position;
                    evict(free_frame, manager_lock);
                    // lock frame in exclusive mode
                    lock_frame(page_id, true);
                    // read frame from disk using frmae's meta data
                    read_frame(page_id, manager_lock);
                    // add frame to fifo/lru queue
                    if (free_frame_pos == BufferFrame::FIFO) {
                        std::lock_guard<std::mutex> fifo_guard(fifo_latch);
                        bufferframes[page_id].position = BufferFrame::FIFO;
                        fifo.push_back(page_id);
                    } else {
                        std::lock_guard<std::mutex> lru_guard(lru_latch);
                        bufferframes[page_id].position = BufferFrame::LRU;
                        lru.push_back(page_id);
                    }

                    // unlock frame in exclusive mode
                    unlock_frame(page_id);
                    // lock frame in user's requested mode return the frame
                    lock_frame(page_id, exclusive);
                    manager_lock.unlock();
                    return bufferframes[page_id];
                } else {
                    // no page can be evicted, throw an error
                    throw buffer_full_error{};
                }
            }
        }
    }
}

void BufferManager::unfix_page(BufferFrame& page, bool is_dirty) {
    page.frame_latch.unlock();
    if (is_dirty) {
        page.state = BufferFrame::DIRTY;
    }
    page.thread_cnt--;
}

std::vector<uint64_t> BufferManager::get_fifo_list() const {
    std::vector<uint64_t> fifo_list;
    for (auto& page_id : fifo) {
        fifo_list.push_back(page_id);
    }

    return fifo_list;
}

std::vector<uint64_t> BufferManager::get_lru_list() const {
    std::vector<uint64_t> lru_list;
    for (auto& page_id : lru) {
        lru_list.push_back(page_id);
    }

    return lru_list;
}

BufferFrame* BufferManager::evict_check() {
    // first check the fifo queue
    for (auto const& page_id : fifo) {
        if (bufferframes[page_id].thread_cnt == 0) {
            return &bufferframes[page_id];
        }
    }

    // second check the lru queue
    for (auto const& page_id : lru) {
        if (bufferframes[page_id].thread_cnt == 0) {
            return &bufferframes[page_id];
        }
    }
    return nullptr;
}

void BufferManager::evict(BufferFrame* evict_frame,
                          std::unique_lock<std::mutex>& manager_lock) {
    auto evict_id = evict_frame->page_id;
    if (evict_frame->state == BufferFrame::DIRTY) {
        write_back_to_disk(evict_frame, manager_lock);
    }
    if (evict_frame->position == BufferFrame::FIFO) {
        fifo.erase(std::find(fifo.begin(), fifo.end(), evict_frame->page_id));
    } else {
        lru.erase(std::find(lru.begin(), lru.end(), evict_frame->page_id));
    }
    bufferframes.erase(evict_id);
}

void BufferManager::lock_frame(uint64_t page_id, bool exclusive) {
    if (!exclusive) {
        bufferframes[page_id].frame_latch.lock_shared();
        bufferframes[page_id].thread_cnt++;
    } else {
        bufferframes[page_id].frame_latch.lock();
        bufferframes[page_id].thread_cnt++;
    }
}

void BufferManager::unlock_frame(uint64_t page_id) {
    bufferframes[page_id].frame_latch.unlock();
    bufferframes[page_id].thread_cnt--;
}

void BufferManager::read_frame(uint64_t page_id,
                               std::unique_lock<std::mutex>& manager_lock) {
    // add a new frame into the hash table
    // bufferframes.emplace(std::piecewise_construct,std::forward_as_tuple(page_id),std::forward_as_tuple(page_id,
    // nullptr));
    bufferframes[page_id].page_id = page_id;
    auto& frame = bufferframes[page_id];
    // std::lock_guard<std::shared_timed_mutex> frame_guard(frame.frame_latch);
    const auto segment_id = get_segment_id(page_id);
    const auto segment_page_id = get_segment_page_id(page_id);

    // read from file and make it writable to write dirty data into the file
    auto file_handle =
        File::open_file(std::to_string(segment_id).c_str(), File::WRITE);

    // calculate the start position we want to read from the current segment
    auto start = segment_page_id * page_size;

    // reserve space for the frame's data
    // frame.data = new char[page_size];
    // std::cout << "free_pos:  " << free_pos << std::endl;
    frame.data = &buffer[free_pos];
    frame.start_pos = free_pos;
    std::memset(frame.data, 0, page_size);
    file_handle->read_block(start, page_size, frame.data);
    free_pos = (free_pos + page_size) % (page_count * page_size);
}

void BufferManager::write_back_to_disk(
    BufferFrame* evict_frame, std::unique_lock<std::mutex>& manager_lock) {
    if (evict_frame->state == BufferFrame::DIRTY) {
        const auto segment_id = get_segment_id(evict_frame->page_id);
        const auto segment_page_id = get_segment_page_id(evict_frame->page_id);

        auto file_handle =
            File::open_file(std::to_string(segment_id).c_str(), File::WRITE);

        file_handle->write_block(evict_frame->data, segment_page_id * page_size,
                                 page_size);
        free_pos = evict_frame->start_pos;
    }
    // delete[] evict_frame->data;
}
} // namespace moderndbs
