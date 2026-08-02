// SPDX-License-Identifier: MIT
#pragma once

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cerrno>
#include <cstddef>
#include <string>
#include <system_error>
#include <utility>

namespace shmspsc {

// macOS caps shm names at PSHMNAMLEN (31); we enforce it everywhere.
inline constexpr std::size_t kMaxShmNameLen = 31;

class ShmSegment {
public:
  ShmSegment() = default;
  ShmSegment(const ShmSegment &) = delete;
  ShmSegment &operator=(const ShmSegment &) = delete;
  ShmSegment(ShmSegment &&o) noexcept { swap(o); }
  ShmSegment &operator=(ShmSegment &&o) noexcept {
    if (this != &o) {
      ShmSegment tmp(std::move(o));
      swap(tmp);
    }
    return *this;
  }
  ~ShmSegment() { reset(); }

  static ShmSegment create(const std::string &name, std::size_t bytes) {
    validateName(name);
    if (bytes == 0) {
      throw std::invalid_argument("ShmSegment::create: bytes must be > 0");
    }

    // ftruncate on a shm object succeeds only once, right after creation, so a
    // stale object of the wrong size is unusable. Always start clean.
    ::shm_unlink(name.c_str());

    const int fd = ::shm_open(name.c_str(), O_CREAT | O_EXCL | O_RDWR, 0600);
    if (fd < 0) {
      throwErrno("shm_open(O_CREAT) " + name, errno);
    }
    if (::ftruncate(fd, static_cast<off_t>(bytes)) != 0) {
      const int e = errno;
      ::close(fd);
      ::shm_unlink(name.c_str());
      throwErrno("ftruncate " + name, e);
    }
    return mapAndClose(fd, name, bytes, true);
  }

  static ShmSegment open(const std::string &name) {
    validateName(name);
    const int fd = ::shm_open(name.c_str(), O_RDWR, 0600);
    if (fd < 0) {
      throwErrno("shm_open " + name + " (is the peer running?)", errno);
    }
    struct stat st {};
    if (::fstat(fd, &st) != 0 || st.st_size <= 0) {
      const int e = errno;
      ::close(fd);
      throwErrno("fstat " + name, e ? e : EINVAL);
    }
    return mapAndClose(fd, name, static_cast<std::size_t>(st.st_size), false);
  }

  static bool exists(const std::string &name) {
    const int fd = ::shm_open(name.c_str(), O_RDONLY, 0600);
    if (fd < 0) {
      return false;
    }
    ::close(fd);
    return true;
  }

  static void remove(const std::string &name) { ::shm_unlink(name.c_str()); }

  void *data() const noexcept { return addr_; }
  std::size_t size() const noexcept { return size_; }
  const std::string &name() const noexcept { return name_; }
  bool valid() const noexcept { return addr_ != nullptr; }
  bool owns() const noexcept { return owner_; }
  void release() noexcept { owner_ = false; }

  void reset() noexcept {
    if (addr_ != nullptr) {
      ::munmap(addr_, size_);
      addr_ = nullptr;
    }
    if (owner_ && !name_.empty()) {
      ::shm_unlink(name_.c_str());
    }
    owner_ = false;
    size_ = 0;
    name_.clear();
  }

private:
  static ShmSegment mapAndClose(int fd, const std::string &name,
                                std::size_t bytes, bool owner) {
    void *addr =
        ::mmap(nullptr, bytes, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    const int e = errno;
    ::close(fd); // the mapping holds its own reference
    if (addr == MAP_FAILED) {
      if (owner) {
        ::shm_unlink(name.c_str());
      }
      throwErrno("mmap " + name, e);
    }
    ShmSegment s;
    s.addr_ = addr;
    s.size_ = bytes;
    s.name_ = name;
    s.owner_ = owner;
    return s;
  }

  static void validateName(const std::string &name) {
    if (name.size() < 2 || name[0] != '/') {
      throw std::invalid_argument("shm name must start with '/': " + name);
    }
    if (name.size() > kMaxShmNameLen) {
      throw std::invalid_argument("shm name exceeds 31 chars: " + name);
    }
    if (name.find('/', 1) != std::string::npos) {
      throw std::invalid_argument("shm name may only contain a leading '/': " +
                                  name);
    }
  }

  [[noreturn]] static void throwErrno(const std::string &what, int e) {
    throw std::system_error(e, std::generic_category(), what);
  }

  void swap(ShmSegment &o) noexcept {
    std::swap(addr_, o.addr_);
    std::swap(size_, o.size_);
    std::swap(owner_, o.owner_);
    name_.swap(o.name_);
  }

  void *addr_ = nullptr;
  std::size_t size_ = 0;
  bool owner_ = false;
  std::string name_;
};

} // namespace shmspsc
