// Day 1 evening spike: proves the C++ Redis client builds, links and can use Streams.
#include <iostream>
#include <iterator>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <sw/redis++/redis++.h>

int main(int argc, char** argv) {
  const std::string url = argc > 1 ? argv[1] : "tcp://127.0.0.1:6379";
  try {
    sw::redis::Redis r(url);
    std::cout << "PING -> " << r.ping() << "\n";
    const std::string stream = "ats:spike";
    r.del(stream);
    auto id = r.xadd(stream, "*", {std::make_pair(std::string("m"), std::string("hello"))});
    std::cout << "XADD -> " << id << "\n";
    using Attrs = std::vector<std::pair<std::string, std::string>>;
    using Item = std::pair<std::string, Attrs>;
    std::vector<Item> items;
    r.xrange(stream, "-", "+", std::back_inserter(items));
    std::cout << "XRANGE -> " << items.size() << " entry, m=" << items.at(0).second.at(0).second << "\n";
    r.del(stream);
    std::cout << "redis spike OK\n";
  } catch (const sw::redis::Error& e) {
    std::cerr << "redis error: " << e.what() << "\n";
    return 1;
  }
}
