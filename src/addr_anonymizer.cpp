#include "addr_anonymizer.h"

void anonymize(std::string &addr) {
    int i = addr.length() - 1;
    char c = addr[i];

    while (c != '.') {
        addr[i] = '\n';
        c = addr[--i];
    }
    addr[i + 1] = 'X';
}