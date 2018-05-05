#ifndef ISAM_ISAM_HPP
#define ISAM_ISAM_HPP

// Vladislav Vancak

#include "block_provider.hpp"
#include <list>
#include <memory>

template<typename TKey, typename TValue>
class isam {
public:
    /* struct pseudo_pair {
         TKey first;
         TValue second;
     };

     class isam_iter {

         isam_iter &operator++() {}

         const isam_iter operator++(int) {}

         pseudo_pair &operator*() {}

         pseudo_pair *operator->() {}

         bool operator==(const pseudo_pair &other) const {}

         bool operator!=(const pseudo_pair &other) const {}
     };

     isam(size_t block_size, size_t overflow_size) {}

     TValue &operator[](TKey key) {}

     isam_iter begin() {
         return isam_iter();
     }

     isam_iter end() {
         return isam_iter();
     }

     */
    class index_tree {
    public:
        void insert_key(TKey &&key_lower, TKey &&key_upper, size_t block_id) {
            auto *tn = new node();

            tn->key_lower = key_lower;
            tn->key_upper = key_upper;
            tn->block_id = block_id;

            // insert as new root
            if (!root) {
                tn->parent = nullptr;
                tn->rb_colour = colour::BLACK;
                root = tn;
                return;
            }

            // find
            node *parent = find(tn->key_lower, tn->key_upper);
            tn->parent = parent;
            tn->rb_colour = colour::RED;

            // insert
            if (key_upper < parent->key_lower) parent->left = tn;
            else parent->right = tn;

            // repair
            fix_tree(tn);
        }

        void delete_key(TKey &&key) {}

        const size_t get_block_id(TKey &&key) const {
            node *current = root;
            while (true) {
                if (current->left && key < current->key_lower) current = current->left;
                else if (current->right && current->key_upper < key) current = current->right;
                else break;
            }
            return current->block_id;
        }

    private:
        enum colour {
            RED, BLACK
        };
        struct node {
            TKey key_upper;
            TKey key_lower;

            size_t block_id = 0;
            colour rb_colour = colour::RED;

            node *parent = nullptr;
            node *left = nullptr;
            node *right = nullptr;
        };

        node *root = nullptr;

        node *find(const TKey &key_lower, const TKey &key_upper) const {
            node *current = root;
            while (true) {
                if (current->left && key_upper < current->key_lower) current = current->left;
                else if (current->right && current->key_upper < key_lower) current = current->right;
                else return current;
            }
        }

        node *get_uncle(node *current) {
            node *parent = current->parent;
            node *grand_parent = parent->parent;
            if (parent == grand_parent->left) return grand_parent->right;
            else return grand_parent->left;
        }

        colour get_colour(node *node) {
            if (!node) return colour::BLACK;
            else return node->rb_colour;
        }

        void fix_tree(node *current) {
            while (true) {
                // no parent => root
                node *parent = current->parent;
                if (!parent) {
                    current->rb_colour = colour::BLACK;
                    root = current;
                    return;
                }

                // no grand parent => parent is root
                node *grand_parent = parent->parent;
                if (!grand_parent) {
                    parent->rb_colour = colour::BLACK;
                    root = parent;
                    return;
                }

                // everything OK
                if (current->rb_colour == colour::BLACK) return;
                if (parent->rb_colour == colour::BLACK) return;

                // red uncle
                node *uncle = get_uncle(current);
                if (get_colour(uncle) == colour::RED) {
                    current = recolour(current, uncle);
                }

                    // black uncle
                else if (parent == grand_parent->left) {
                    if (current == parent->left) current = rotate_LL(current);
                    else current = rotate_LR(current);
                }

                else {
                    if (current == parent->left) current = rotate_RL(current);
                    else current = rotate_RR(current);
                }
            }
        }

        node *recolour(node *current, node *uncle) {
            node *parent = current->parent;
            node *grand_parent = parent->parent;

            grand_parent->rb_colour = colour::RED;
            parent->rb_colour = colour::BLACK;
            uncle->rb_colour = colour::BLACK;

            return grand_parent;
        }

        node *rotate_LL(node *x) {
            node *p = x->parent;
            node *g = p->parent;

            // Right subtree of P to the left subtree of G
            g->left = p->right;
            if (g->left) g->left->parent = g;

            // P is the subtree root
            p->parent = g->parent;
            if (g->parent) {
                if (g == g->parent->left) g->parent->left = p;
                else g->parent->right = p;
            }

            // P right is G
            p->right = g;
            g->parent = p;

            // Colour changes
            p->rb_colour = colour::BLACK;
            g->rb_colour = colour::RED;

            return p;
        }

        node *rotate_LR(node *x) {
            node *p = x->parent;
            node *g = p->parent;

            // Left subtree of X to the right subtree of P
            p->right = x->left;
            if (p->right) p->right->parent = p;

            // Right subtree of X to the left subtree of G
            g->left = x->right;
            if (g->left) g->left->parent = g;

            // X is the new subtree root
            x->parent = g->parent;
            if (g->parent) {
                if (g == g->parent->left) g->parent->left = x;
                else g->parent->right = x;
            }

            // X left is P
            x->left = p;
            p->parent = x;

            // X right is G
            x->right = g;
            g->parent = x;

            // Colour changes
            x->rb_colour = colour::BLACK;
            g->rb_colour = colour::RED;

            return x;
        }

        node *rotate_RR(node *x) {
            node *p = x->parent;
            node *g = p->parent;

            // Left subtree of P into right subtree of G
            g->right = p->left;
            if (g->right) g->right->parent = g;

            // P is the subtree root
            p->parent = g->parent;
            if (g->parent) {
                if (g == g->parent->left) g->parent->left = p;
                else g->parent->right = p;
            }

            // P right is G
            p->left = g;
            g->parent = p;

            // Colour change
            p->rb_colour = colour::BLACK;
            g->rb_colour = colour::RED;

            return p;
        }

        node *rotate_RL(node *x) {
            node *p = x->parent;
            node *g = p->parent;

            // Left subtree of X into right subtree of G
            g->right = x->left;
            if (g->right) g->right->parent = g;

            // Right subtree of X into left subtree of P
            p->left = x->right;
            if (p->left) p->left->parent = p;

            // X is the subtree root
            x->parent = g->parent;
            if (g->parent) {
                if (g == g->parent->left) g->parent->left = x;
                else g->parent->right = x;
            }

            // X left is G
            x->left = g;
            g->parent = x;

            // X right is P
            x->right = p;
            p->parent = x;

            // rb_colour change
            x->rb_colour = colour::BLACK;
            g->rb_colour = colour::RED;

            return x;
        }
    };

    //std::list<size_t> file_blocks;
    //std::list<pseudo_pair> overflow;
};

#endif // ISAM_ISAM_HPP
