#ifndef TEST_01_SPLIT_
#define TEST_01_SPLIT_

#include<tuple>
#include<string>
#include<sstream>

const size_t CR = 13;

namespace splitter {
    // Return value of the split function
    template<typename... TA>
    struct Storage {
        std::tuple<TA...> internal_tuple;

        Storage(std::tuple<TA...> &&tpl) : internal_tuple(tpl) {}

        Storage(const Storage &x) = delete;

        Storage &operator=(const Storage &x) = delete;

        Storage(Storage &&other) = default;

        Storage &operator=(Storage &&x) noexcept = default;
    };

    // Internal Implementation namespace
    namespace splitter_impl {

        template<typename T>
        inline void parse_value_delimiter(std::istream &in, T &lvalue, char &delimiter, bool end_of_storage = false) {
            // Stream Read
            std::string stream_segment_string;
            std::getline(in, stream_segment_string, delimiter);

            if (in.eof() && !end_of_storage) throw std::logic_error("Stream read: Unexpected EOF");

            // String Stream Read
            std::stringstream stream_segment;
            stream_segment << stream_segment_string;
            stream_segment >> lvalue;

            // Checks
            if (stream_segment.fail()) throw std::logic_error("Stream read: Value parsing failed");
            int_least32_t last_value = stream_segment.get();
            if (stream_segment.eof()) return;
            if (last_value == CR && stream_segment.peek() == EOF) return;
            throw std::logic_error("Stream read: Unexpected character(s)");
        }

        // Base
        template<typename... Args>
        struct Processor {};

        // Default (i.e. wrong)
        template<typename T, typename... TR>
        struct Processor<T, TR...> {
            template<typename... TA>
            static void parse_value(std::istream &in, const Storage<TA...> &storage) {

                Processor<TR...>::parse_value(in, storage);

                static_assert(!std::is_lvalue_reference<T>(), "Unseparated lvalue");
                static_assert(std::is_lvalue_reference<T>(), "Non-char delimiter");

                // Should not get here during runtime
                throw std::logic_error("Storage Processing Error");
            }
        };

        // Delimiter
        template<typename...TR>
        struct Processor<char, TR...> {
            template<typename...TA>
            static void parse_value(std::istream &in, const Storage<TA...> &storage) {
                // Storage Read
                const size_t N = sizeof...(TA) - sizeof...(TR);
                char delimiter = std::get<N - 1>(storage.internal_tuple);

                // Stream Read
                int_least32_t value = in.get();
                if (in.eof()) throw std::logic_error("Stream read: Unexpected EOF");
                if (in.fail()) throw std::logic_error("Stream read: Delimiter parsing failed");
                if (delimiter != value) throw std::logic_error("Stream read: Invalid delimiter");

                // Proceed
                Processor<TR...>::parse_value(in, storage);
            }
        };

        // Lvalue + Delimiter
        template<typename T, typename...TR>
        struct Processor<T &, char, TR...> {
            template<typename...TA>
            static void parse_value(std::istream &in, const Storage<TA...> &storage) {
                // Storage Read
                const size_t N = sizeof...(TA) - sizeof...(TR);
                auto &lvalue = std::get<N - 2>(storage.internal_tuple);
                char delimiter = std::get<N - 1>(storage.internal_tuple);

                // Process
                parse_value_delimiter(in, lvalue, delimiter);

                // Proceed
                Processor<TR...>::parse_value(in, storage);
            }
        };

        // Lvalue + End-Of-Storage
        template<typename T>
        struct Processor<T &> {
            template<typename...TA>
            static void parse_value(std::istream &in, const Storage<TA...> &storage) {

                // Storage Read
                const size_t N = sizeof...(TA);
                auto &lvalue = std::get<N - 1>(storage.internal_tuple);
                char delimiter = '\n';

                // Process
                parse_value_delimiter(in, lvalue, delimiter, true);
            }
        };

        // Void
        template<>
        struct Processor<> {
            template<typename...TA>
            static void parse_value(std::istream &in, const Storage<TA...> &storage) {
                if (in.good()) in.peek();
            }
        };
    }

    template<typename... TA>
    inline Storage<TA...> split(TA &&... params) {
        std::tuple<TA...> tpl(params...);
        return Storage<TA...>(std::move(tpl));
    }

    template<typename... TA>
    inline std::istream &operator>>(std::istream &in, const Storage<TA...> &storage) {
        splitter_impl::Processor<TA...>::parse_value(in, storage);
    }
}

#endif