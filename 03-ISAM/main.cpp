#include <iostream>
#include "isam.hpp"

using std::string;
using std::cout;

int main() {
    {
        std::cout << " === MY - ORDER === " << std::endl;
        isam<int, int> index(2, 2);
        for (int i = 0; i < 1000; ++i) index[i] = i;
    }

    {
        std::cout << " === MY - REVERSE === " << std::endl;
        isam<int, int> index(2, 2);
        for (int i = 1000; i > 0; --i) index[i] = i;
    }
    {
        std::cout << " === MY === " << std::endl;

        isam<int, string> index(3, 2);
        index[5] = string("5");
        index[2] = string("2");
        index[4] = string("4");
        index[-1] = string("-1");
        index[-2] = string("-2");
        index[-4] = string("-4");
        index[1000] = string("1000");


        std::cout << index[-1] << std::endl;
        std::cout << index[5] << std::endl;
        std::cout << index[2] << std::endl;
        std::cout << index[4] << std::endl;
        std::cout << index[0] << std::endl;

        std::cout << " === FOREACH === " << std::endl;

        for (auto &&it : index) {
            cout << it.first << ":" << it.second << " " << std::endl;
        }

        std::cout << " === CONST MY === " << std::endl;

        const isam<int, string> *temp = &index;
        std::cout << (*temp)[-4000] << std::endl;
        index[0] = std::string("0");
        std::cout << (*temp)[0] << std::endl;

        for (auto &&it : *temp) {
            auto it_ii(it);
            std::cout << it_ii.first << ":" << it_ii.second << std::endl;

            auto it_iii = it;
            std::cout << it_iii.first << ":" << it_iii.second << std::endl;
        }
    }
    {
        std::cout << " === FIRST === " << std::endl;

        isam<int, string *> index(1, 2);
        index[5] = new string("5");
        index[2] = new string("2");
        index[4] = new string("4"); //any records in the overflow space?
        for (auto &&it : index) {
            cout << it.first << ":" << *it.second << " ";
        }
        std::cout << std::endl;
        //output: 2:2 4:4 5:5
    }
    {
        std::cout << " === SECOND === " << std::endl;

        isam<int, double> index(1, 1);
        index[1] = 1;
        {
            auto it = index.begin();
            it->second = 2;
        }
        std::cout << index[1] << std::endl;
    }
    return 0;
}
