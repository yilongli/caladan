#include "shuffle_util.h"

#include <net/if.h>
#include <netinet/in.h>
#include <sys/ioctl.h>
#include <unistd.h>

/**
 * Use ioctl to obtain the IP address of a local network interface.
 *
 * \param ifname
 *      Symbolic name of the network interface.
 * \return
 *      IP address of the interface on success; -1 on failure.
 */
int get_local_ip(const std::string& ifname) {
    struct ifreq ifr = {};
    ifname.copy(ifr.ifr_name, ifname.length());
    ifr.ifr_name[ifname.length() + 1] = 0;
    if (ifname.length() >= sizeof(ifr.ifr_name)) {
        return -1;
    }

    int fd = socket(AF_INET, SOCK_DGRAM, 0);
    if (fd == -1) {
        return -1;
    }

    if (ioctl(fd, SIOCGIFADDR, &ifr) == -1) {
        close(fd);
        return -1;
    }
    return be32toh(((struct sockaddr_in*)&ifr.ifr_addr)->sin_addr.s_addr);
}

int
parse_netaddr(const char* str, struct netaddr* addr) {
    uint8_t a, b, c, d;
    uint16_t port;
    if (sscanf(str, "%hhu.%hhu.%hhu.%hhu:%hu", &a, &b, &c, &d, &port) != 5) {
        return -1;
    }
    addr->ip = (((uint32_t) a << 24) | ((uint32_t) b << 16) |
                ((uint32_t) c << 8) | (uint32_t) d);
    addr->port = port;
    return 0;
}

std::string
netaddr_to_str(struct netaddr addr) {
    char buf[32] = {};
    snprintf(buf, 32, "%d.%d.%d.%d:%u", ((addr.ip >> 24) & 0xff),
            ((addr.ip >> 16) & 0xff), ((addr.ip >> 8) & 0xff), (addr.ip & 0xff),
            addr.port);
    return buf;
}
