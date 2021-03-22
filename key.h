#ifndef KEY_H
#define KEY_H

#include <stdint.h>

#include <string>

const size_t key_max_len = 255;

inline size_t key_size(std::string s) {
  size_t ss = s.size();
  if (ss > key_max_len) ss = key_max_len;

  return ss + 1;
}

struct key {
  const uint8_t *b;

  key(const uint8_t *bb)
    : b(bb) {}

  key(uint8_t *bb, std::string s)
    : b(bb)
  {
    size_t ss = s.size();
    if (ss > key_max_len) ss = key_max_len;

    *bb = ss;
    memcpy(bb + 1, s.data(), ss);
  }

  key(const key &k) : b(k.b) {}

  uint8_t size() {
    return 1 + *b;
  }

  const uint8_t *data() {
    return b;
  }

  const char *c_str() {
    return (const char *) b + 1;
  }

  uint8_t len() {
    return *b;
  }

  std::string str() {
    return std::string(b + 1, b + 1 + *b);
  }

  bool operator==(std::string &s) const {
    if (*b != s.size()) return false;
    return memcmp(b + 1, s.data(), *b) == 0;
  }

  bool operator==(const key &o) const {
    if (*b != *o.b) return false;
    return memcmp(b + 1, o.b + 1, *b) == 0;
  }

  int compare(const std::string &s) const {
    uint8_t l = 0;;
    uint8_t ol = s.size();;
    const char *od = s.data();;

    while (l < *b && l < ol) {
      if (b[1+l] < od[l]) {
        return -1;
      } else if (b[1+l] > od[l]) {
        return 1;
      } else {
        l++;
      }
    }

    if (l == *b && l == ol) {
      return 0;
    } else if (l == *b) {
      return -1;
    } else {
      return 1;
    }
  }

  bool operator<=(const std::string &s) const {
    uint8_t l = 0;;
    uint8_t ol = s.size();;
    const char *od = s.data();;

    while (l < *b && l < ol) {
      if (b[1+l] < od[l]) {
        return true;
      } else if (b[1+l] > od[l]) {
        return false;
      } else {
        l++;
      }
    }

    return true;
  }

  bool operator>=(const std::string &s) const {
    uint8_t l = 0;;
    uint8_t ol = s.size();;
    const char *od = s.data();;

    while (l < *b && l < ol) {
      if (b[1+l] > od[l]) {
        return true;
      } else if (b[1+l] < od[l]) {
        return false;
      } else {
        l++;
      }
    }

    return true;
  }

  bool operator<(const std::string &s) const {
    uint8_t l = 0;;
    uint8_t ol = s.size();;
    const char *od = s.data();;

    while (l < *b && l < ol) {
      if (b[1+l] < od[l]) {
        return true;
      } else if (b[1+l] > od[l]) {
        return false;
      } else {
        l++;
      }
    }

    return l == *b;
  }

  bool operator<(const key &o) const {
    uint8_t l = 0;;

    while (l < *b && l < *o.b) {
      if (b[1+l] < o.b[1+l]) {
        return true;
      } else if (b[1+l] > o.b[1+l]) {
        return false;
      } else {
        l++;
      }
    }

    return l == *b;
  }
};


#endif

