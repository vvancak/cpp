#include <iostream>
#include "isam.hpp"

using std::string;
using std::cout;

int main() {
/*
    std::cout << " === MY === " << std::endl;

    isam<int, string *> index(2, 2);
    index[5] = new string("5");
    index[2] = new string("2");
    index[4] = new string("4");
    index[500] = new string("500");
    index[200] = new string("200");
    index[400] = new string("400");
    index[-1] = new string("-1");
    index[-2] = new string("-2");
    index[-4] = new string("-4");
    index[1000] = new string("1000");

    //any records in the overflow space?
    std::cout << *(index[-1]) << std::endl;
    std::cout << *(index[5]) << std::endl;
    std::cout << *(index[2]) << std::endl;
    std::cout << *(index[4]) << std::endl;
    std::cout << *(index[200]) << std::endl;
    std::cout << *(index[400]) << std::endl;
    std::cout << *(index[500]) << std::endl;
    std::cout << *(index[-2]) << std::endl;
    std::cout << *(index[-4]) << std::endl;

    std::cout << " === FOREACH === " << std::endl;

    for (auto &&it : index) {
        cout << it.first << ":" << *it.second << " " << std::endl;
    }
 */
    std::cout << " === FIRST === " << std::endl;

    isam<int, string *> index(1, 2);
    index[5] = new string("5");
    index[2] = new string("2");
    index[4] = new string("4"); //any records in the overflow space?
    for (auto &&it : index) {
        cout << it.first << ":" << *it.second << " ";
    }
//output: 2:2 4:4 5:5

/*
    std::cout << " === SECOND === " << std::endl;

    isam<int, double> indexII(1, 1);
    indexII[1] = 1;
    {
        auto it = indexII.begin();
        it->second = 2;
    }
    std::cout << indexII[1] << std::endl;
*/
    return 0;
}
