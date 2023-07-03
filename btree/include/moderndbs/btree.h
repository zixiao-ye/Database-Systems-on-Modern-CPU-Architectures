#ifndef INCLUDE_MODERNDBS_BTREE_H
#define INCLUDE_MODERNDBS_BTREE_H

#include "moderndbs/buffer_manager.h"
#include "moderndbs/defer.h"
#include "moderndbs/segment.h"
#include <cassert>
#include <cstring>
#include <iostream>
#include <mutex>
#include <optional>

namespace moderndbs {

template <typename KeyT, typename ValueT, typename ComparatorT, size_t PageSize>
struct BTree : public Segment {
    struct Node {
        /// The level in the tree.
        uint16_t level;
        /// The number of children.
        uint16_t count;

        // Constructor
        Node(uint16_t level, uint16_t count) : level(level), count(count) {}

        /// Is the node a leaf node?
        bool is_leaf() const { return level == 0; }
    };

    struct InnerNode : public Node {
        /// The capacity of a node.
        /// For inner node, there should be one more child in the end
        static constexpr uint32_t kCapacity =
            (PageSize - sizeof(uint64_t)) / (sizeof(KeyT) + sizeof(uint64_t));

        /// The keys.
        KeyT keys[kCapacity];

        /// The values.
        uint64_t children[kCapacity + 1]; // the number of children should be
                                          // one more than keys

        /// Constructor.
        InnerNode() : Node(0, 0) {}

        /// Get the index of the first key that is not less than than a provided
        /// key.
        /// @param[in] key          The key that should be inserted.
        std::pair<uint32_t, bool> lower_bound(const KeyT& key) {
            if (this->count == 0) {
                return {0, false};
            }

            ComparatorT comp = ComparatorT();
            uint32_t first = 0;
            uint32_t count = this->count - 1;

            while (count > 0) {
                uint32_t index = first;
                uint32_t step = count / 2;
                index += step;

                if (comp(keys[index], key)) {
                    first = ++index;
                    count -= step + 1;
                } else {
                    count = step;
                }
            }
            bool found =
                (first >= 0 && first < this->count && keys[first] == key);
            return {first, found};
        }

        uint64_t lookup(const KeyT& key) {
            auto [first, found] = this->lower_bound(key);
            if (first == this->count) {
                // std::cout << first << std::endl;
                return this->children[first - 1];
            }
            return this->children[first];
        }

        /// @brief the first time insert into an inner node, need both left and
        /// right page id
        /// @param key         the key will be inserted
        /// @param left        left page id
        /// @param right       right page id
        void first_insert(const KeyT& key, uint64_t left, uint64_t right) {
            children[0] = left;
            children[1] = right;
            keys[0] = key;
            keys[1] = key;
            this->count = 2;
        }

        /// Insert a key.
        /// @param[in] key          The key that should be inserted.
        /// @param[in] split_page   The child that should be inserted.
        void insert_split(const KeyT& key, uint64_t split_page) {
            auto [first, found] = this->lower_bound(key);

            // new key and value to insert into.
            uint32_t num_after = this->count - first - 1;
            std::memmove(&keys[first + 1], &keys[first],
                         (num_after) * sizeof(KeyT));
            std::memmove(&children[first + 2], &children[first + 1],
                         num_after * sizeof(ValueT));
            keys[first] = key;
            children[first + 1] = split_page;

            this->count++;
        }

        /// Split the node.
        /// @param[in] buffer       The buffer for the new page.
        /// @return                 The separator key.
        KeyT split(std::byte* buffer) {
            assert(this->count == kCapacity + 1);
            KeyT separator = keys[(this->count - 1) / 2];
            auto another_leaf = reinterpret_cast<InnerNode*>(buffer);

            another_leaf->count = this->count - ((this->count - 1) / 2 + 1);
            this->count = (this->count - 1) / 2 + 1;
            // std::cout << another_leaf->count << std::endl;

            std::memmove(&another_leaf->keys, &keys[this->count],
                         another_leaf->count * sizeof(KeyT));
            std::memmove(&another_leaf->children, &children[this->count],
                         another_leaf->count * sizeof(ValueT));
            assert(this->keys[this->count - 1] == separator);
            return separator;
        }

        /// Returns the keys.
        std::vector<KeyT> get_key_vector() {
            std::vector<KeyT> dest(keys, keys + this->count - 1);
            return dest;
        }
    };

    struct LeafNode : public Node {
        /// The capacity of a node.
        /// For leaf nodes, there are same number of keys and TIDs
        static constexpr uint32_t kCapacity =
            (PageSize - 2 * sizeof(uint64_t)) /
            (sizeof(KeyT) + sizeof(uint64_t));

        /// The keys.
        KeyT keys[kCapacity]; // adjust this
        /// The values.
        ValueT values[kCapacity]; // adjust this

        /// Constructor.
        LeafNode() : Node(0, 0) {}

        /// Get the index of the first key that is not less than than a provided
        /// key.
        std::pair<uint32_t, bool> lower_bound(const KeyT& key) {
            if (this->count == 0) {
                return {0, false};
            }

            ComparatorT comp = ComparatorT();
            uint32_t first = 0;
            uint32_t count = this->count;

            while (count > 0) {
                uint32_t index = first;
                uint32_t step = count / 2;
                index += step;

                if (comp(keys[index], key)) {
                    first = ++index;
                    count -= step + 1;
                } else {
                    count = step;
                }
            }
            bool found =
                (keys[first] == key && first >= 0 && first < this->count);
            return {first, found};
        }

        std::optional<ValueT> lookup(const KeyT& key) {
            const auto [first, found] = this->lower_bound(key);
            if (found) {
                // std::cout << "found!!!!!" << std::endl;
                return this->values[first];
            } else {
                return std::nullopt;
            }
        }

        /// Insert a key.
        /// @param[in] key          The key that should be inserted.
        /// @param[in] value        The value that should be inserted.
        void insert(const KeyT& key, const ValueT& value) {
            if (this->count == 0) {
                keys[0] = key;
                values[0] = value;
            } else {
                auto [first, found] = this->lower_bound(key);
                if (found) {
                    // If found the key, update the value.
                    values[first] = value;
                    return;
                } else {
                    // new key and value to insert into.
                    uint32_t num_after = this->count - first;
                    std::memmove(&keys[first + 1], &keys[first],
                                 num_after * sizeof(KeyT));
                    std::memmove(&values[first + 1], &values[first],
                                 num_after * sizeof(ValueT));
                    keys[first] = key;
                    values[first] = value;
                }
            }
            this->count++;
        }

        /// Erase a key.
        void erase(const KeyT& key) {
            if (this->count == 0)
                return;

            auto [first, found] = this->lower_bound(key);
            if (found) {
                uint32_t num_after = this->count - first - 1;
                std::memmove(&keys[first], &keys[first + 1],
                             num_after * sizeof(KeyT));
                std::memmove(&values[first], &values[first + 1],
                             num_after * sizeof(ValueT));
                this->count--;
            }
        }

        /// Split the node.
        /// @param[in] buffer       The buffer for the new page.
        /// @return                 The separator key.
        KeyT split(std::byte* buffer) {
            KeyT separator = keys[this->count / 2];
            auto another_leaf = reinterpret_cast<BTree::LeafNode*>(buffer);

            another_leaf->count = this->count - (this->count / 2 + 1);
            this->count = this->count / 2 + 1;
            // std::cout << another_leaf->count << std::endl;

            std::memmove(&another_leaf->keys, &keys[this->count],
                         another_leaf->count * sizeof(KeyT));
            std::memmove(&another_leaf->values, &values[this->count],
                         another_leaf->count * sizeof(ValueT));

            return separator;
        }

        /// Returns the keys.
        std::vector<KeyT> get_key_vector() {
            std::vector<KeyT> dest(keys, keys + this->count);
            return dest;
        }

        /// Returns the values.
        std::vector<ValueT> get_value_vector() {
            std::vector<ValueT> dest(values, values + this->count);
            return dest;
        }
    };

    /// The root.
    uint64_t root;         // root page
    uint64_t next_page;    // next available page id
    uint64_t leaf_page_id; // used only once, when we need to record the page id
                           // of the old root node
    bool is_empty;
    std::mutex insert_mutex;

    /// Constructor.
    BTree(uint16_t segment_id, BufferManager& buffer_manager)
        : Segment(segment_id, buffer_manager), is_empty(true) {
        next_page = (uint64_t)segment_id << 48;
    }

    /// Destructor.
    ~BTree() = default;

    /// @brief Lookup the leaf page for the corresponding key
    /// @param key          The key that should be searched.
    /// @param insert_mode       under insert mode, need do some safe split
    /// check.
    /// @return             return the parant page and the leaf page
    std::pair<BufferFrame*, BufferFrame*> lookup_leaf_page(const KeyT& key,
                                                           bool insert_mode) {
        // concurrent control by lock coupling

        // go through the tree
        uint64_t parent_id = root;
        BufferFrame* parent_page = &buffer_manager.fix_page(parent_id, true);
        auto parent_node =
            reinterpret_cast<BTree::Node*>(parent_page->get_data());

        // check whether the root is a single leaf node
        if (parent_node->is_leaf()) {
            this->leaf_page_id = parent_id;
            return {parent_page, parent_page};
        } else {
            // the root is an inner node, then traverse the tree using lock
            // coupling
            auto parent_inner_node =
                reinterpret_cast<BTree::InnerNode*>(parent_page->get_data());

            if (insert_mode) {
                if (parent_inner_node->count >= InnerNode::kCapacity + 1) {
                    // need new page for split
                    uint64_t new_inner_page_id = next_page++;
                    BufferFrame* new_inner_page =
                        &buffer_manager.fix_page(new_inner_page_id, true);
                    auto new_inner_node = reinterpret_cast<InnerNode*>(
                        new_inner_page->get_data());
                    new_inner_node->level = parent_inner_node->level;

                    // split the original root
                    KeyT separator_key = parent_inner_node->split(
                        (std::byte*)new_inner_page->get_data());

                    BufferFrame& new_root_page =
                        buffer_manager.fix_page(next_page, true);
                    root = next_page++;
                    auto new_root_node =
                        reinterpret_cast<InnerNode*>(new_root_page.get_data());
                    new_root_node->level = parent_inner_node->level + 1;
                    new_root_node->first_insert(separator_key, parent_id,
                                                new_inner_page_id);
                    buffer_manager.unfix_page(new_root_page, true);

                    // lock coupling(unfix one of the two splitted node)
                    ComparatorT comp = ComparatorT();
                    if (comp(separator_key, key)) {
                        buffer_manager.unfix_page(*parent_page, true);
                        parent_id = new_inner_page_id;
                        parent_page = new_inner_page;
                        parent_inner_node = new_inner_node;
                    } else {
                        buffer_manager.unfix_page(*new_inner_page, true);
                    }
                }
            }

            uint64_t child_id = parent_inner_node->lookup(key);

            BufferFrame* child_page = &buffer_manager.fix_page(child_id, true);
            auto child_node =
                reinterpret_cast<BTree::Node*>(child_page->get_data());

            while (!child_node->is_leaf()) {
                auto child_inner_node =
                    reinterpret_cast<InnerNode*>(child_page->get_data());

                if (insert_mode) {
                    if (child_inner_node->count >= InnerNode::kCapacity + 1) {
                        // need new page for split
                        uint64_t new_inner_page_id = next_page++;
                        BufferFrame* new_inner_page =
                            &buffer_manager.fix_page(new_inner_page_id, true);
                        auto new_inner_node = reinterpret_cast<InnerNode*>(
                            new_inner_page->get_data());
                        new_inner_node->level = child_inner_node->level;

                        // split the original root
                        KeyT separator_key = child_inner_node->split(
                            (std::byte*)new_inner_page->get_data());

                        parent_inner_node->insert_split(separator_key,
                                                        new_inner_page_id);

                        // lock coupling(unfix one of the two splitted node)
                        ComparatorT comp = ComparatorT();
                        if (comp(separator_key, key)) {
                            buffer_manager.unfix_page(*child_page, true);
                            child_id = new_inner_page_id;
                            child_page = new_inner_page;
                            child_inner_node = new_inner_node;
                        } else {
                            buffer_manager.unfix_page(*new_inner_page, true);
                        }
                    }
                }

                buffer_manager.unfix_page(*parent_page, true);

                parent_id = child_id;
                parent_page = child_page;
                parent_inner_node = child_inner_node;
                parent_node = child_node;

                // get the child at next level
                child_id = parent_inner_node->lookup(key);
                child_page = &buffer_manager.fix_page(child_id, true);
                child_node = reinterpret_cast<Node*>(child_page->get_data());
            }
            this->leaf_page_id = child_id;
            buffer_manager.unfix_page(*parent_page, true);
            return {parent_page, child_page};
        }
    }

    /// Lookup an entry in the tree.
    /// @param[in] key      The key that should be searched.
    /// @return             Whether the key was in the tree.
    std::optional<ValueT> lookup(const KeyT& key) {
        auto [parent_page, leaf_page] = lookup_leaf_page(key, false);
        auto leaf_node = reinterpret_cast<LeafNode*>(leaf_page->get_data());

        auto result = leaf_node->lookup(key);
        if (parent_page == leaf_page) {
            buffer_manager.unfix_page(*parent_page, true);
        } else {
            buffer_manager.unfix_page(*leaf_page, true);
        }

        return result;
    }

    /// Erase an entry in the tree.
    /// @param[in] key      The key that should be searched.
    void erase(const KeyT& key) {
        // 1. lookup the appropriate leaf page
        auto [parent_page, leaf_page] = lookup_leaf_page(key, false);
        auto leaf_node = reinterpret_cast<LeafNode*>(leaf_page->get_data());
        // 2. remove the entry from the current page
        leaf_node->erase(key);

        if (parent_page == leaf_page) {
            buffer_manager.unfix_page(*parent_page, true);
        } else {
            buffer_manager.unfix_page(*leaf_page, true);
        }

        /// TODO: pages balance
        /// 3. is the current page at least half full? if yes, stop.
        /// 4. is the neighboring page more than half full? if yes, balance both
        /// pages, update separator, and stop
        /// 5. merge neighboring page into current page
        /// remove the separator from the parant, continue with 3
    }

    /// Inserts a new entry into the tree.
    /// @param[in] key      The key that should be inserted.
    /// @param[in] value    The value that should be inserted.
    void insert(const KeyT& key, const ValueT& value) {
        std::lock_guard<std::mutex> lock(insert_mutex);
        // 1. If empty, create a new root
        if (is_empty) {
            root = next_page++;
            auto& root_page = buffer_manager.fix_page(root, true);
            auto root_node =
                reinterpret_cast<BTree::LeafNode*>(root_page.get_data());
            root_node->level = 0;
            root_node->insert(key, value);
            Defer root_page_unfix(
                [&]() { buffer_manager.unfix_page(root_page, true); });
            is_empty = false;

            return;
        }

        // 2. look up the appropriate leaf page
        auto [parent_page, leaf_page] = lookup_leaf_page(key, true);
        auto leaf_node = reinterpret_cast<LeafNode*>(leaf_page->get_data());

        // 3. is the leaf full?
        if (leaf_node->count == LeafNode::kCapacity) {
            auto new_leaf_page_id = next_page++;
            BufferFrame* new_leaf_page =
                &buffer_manager.fix_page(new_leaf_page_id, true);
            LeafNode* new_leaf_node =
                reinterpret_cast<LeafNode*>(new_leaf_page->get_data());
            new_leaf_node->level = 0;

            // split the original node
            KeyT separator_key =
                leaf_node->split((std::byte*)new_leaf_page->get_data());

            // whether the current node is the root node
            if (parent_page == leaf_page) {
                BufferFrame& new_root_page =
                    buffer_manager.fix_page(next_page, true);

                InnerNode* new_root_node =
                    reinterpret_cast<InnerNode*>(new_root_page.get_data());
                new_root_node->level = 1;
                new_root_node->first_insert(separator_key, root,
                                            new_leaf_page_id);
                root = next_page++;
                buffer_manager.unfix_page(new_root_page, true);
            } else {
                // no need to add an new root page.
                InnerNode* parent_inner_node =
                    reinterpret_cast<InnerNode*>(parent_page->get_data());
                parent_inner_node->insert_split(separator_key,
                                                new_leaf_page_id);
            }

            // After splitting, insert the key and value
            const ComparatorT comparator = ComparatorT();
            if (comparator(separator_key, key)) {
                new_leaf_node->insert(key, value);
            } else {
                leaf_node->insert(key, value);
            }
            buffer_manager.unfix_page(*new_leaf_page, true);
            buffer_manager.unfix_page(*leaf_page, true);

        } else {
            // the leaf node is not full
            leaf_node->insert(key, value);
            // std::cout << key << std::endl;
            if (parent_page == leaf_page) {
                buffer_manager.unfix_page(*parent_page, true);
            } else {
                buffer_manager.unfix_page(*leaf_page, true);
            }
        }
    }
};

} // namespace moderndbs

#endif
