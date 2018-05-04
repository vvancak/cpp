#include <iostream>
#include "isam.hpp"

using std::string
using std::cout

int main() {
    isam<int, string *> index(1, 2);
    index[5] = new string("5");
    index[2] = new string("2");
    index[4] = new string("4"); //any records in the overflow space?
    for (auto &&it : index) {
        cout << it.first << ":" << *it.second << " ";
    }
    //output: 2:2 4:4 5:5

    return 0;
}
