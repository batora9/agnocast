// SPDX-License-Identifier: Apache-2.0
#include "metadata_store.hpp"

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cerrno>

TopicWrapper::~TopicWrapper()
{
  if (shm != nullptr) munmap(shm, sizeof(*shm));
  if (shm_fd >= 0) close(shm_fd);
}

bool MetadataStore::create_segment(int * out_fd, AgnocastTopicShm ** out_shm)
{
  const int fd = memfd_create("agnocast_topic", MFD_CLOEXEC | MFD_ALLOW_SEALING);
  if (fd < 0) return false;
  void * mapped = MAP_FAILED;
  if (
    ftruncate(fd, sizeof(AgnocastTopicShm)) == 0 &&
    fcntl(fd, F_ADD_SEALS, F_SEAL_SHRINK | F_SEAL_GROW | F_SEAL_SEAL) == 0) {
    mapped = mmap(nullptr, sizeof(AgnocastTopicShm), PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
  }
  if (mapped == MAP_FAILED) {
    close(fd);
    return false;
  }
  auto * shm = static_cast<AgnocastTopicShm *>(mapped);
  if (topic_shm_init(shm) < 0) {
    munmap(mapped, sizeof(AgnocastTopicShm));
    close(fd);
    return false;
  }
  *out_fd = fd;
  *out_shm = shm;
  return true;
}

TopicWrapper * MetadataStore::find_or_create_topic(const TopicKey & key)
{
  auto it = topic_map_.find(key);
  if (it != topic_map_.end()) {
    return it->second.get();
  }
  auto wrapper = std::make_unique<TopicWrapper>();
  if (!create_segment(&wrapper->shm_fd, &wrapper->shm)) return nullptr;
  wrapper->key = key.name;
  wrapper->domain_id = key.domain_id;
  auto * ptr = wrapper.get();
  topic_map_.emplace(key, std::move(wrapper));
  return ptr;
}

int MetadataStore::adopt_topic(const TopicKey & key, int shm_fd, TopicWrapper ** out)
{
  struct FdCloser
  {
    int fd;
    ~FdCloser()
    {
      if (fd >= 0) close(fd);
    }
  } closer{shm_fd};
  *out = nullptr;
  if (shm_fd < 0) return -EINVAL;

  struct stat st
  {
  };
  if (fstat(shm_fd, &st) < 0) return -errno;
  // Only a sealed segment of the right size: the daemon must not be the one
  // that takes SIGBUS when a client shrinks it.
  const int seals = fcntl(shm_fd, F_GET_SEALS);
  if (
    st.st_size != static_cast<off_t>(sizeof(AgnocastTopicShm)) || seals < 0 ||
    (seals & (F_SEAL_SHRINK | F_SEAL_GROW)) != (F_SEAL_SHRINK | F_SEAL_GROW)) {
    return -EINVAL;
  }

  if (auto it = topic_map_.find(key); it != topic_map_.end()) {
    struct stat cur
    {
    };
    if (fstat(it->second->shm_fd, &cur) < 0) return -errno;
    if (cur.st_ino != st.st_ino || cur.st_dev != st.st_dev) return -EEXIST;
    *out = it->second.get();
    return 0;
  }

  void * mapped =
    mmap(nullptr, sizeof(AgnocastTopicShm), PROT_READ | PROT_WRITE, MAP_SHARED, shm_fd, 0);
  if (mapped == MAP_FAILED) return -errno;
  auto * shm = static_cast<AgnocastTopicShm *>(mapped);
  // A segment of another build, or one this daemon's predecessor had already
  // replaced, is not taken back.
  if (
    __atomic_load_n(&shm->magic, __ATOMIC_ACQUIRE) != AGNOCAST_TOPIC_SHM_MAGIC ||
    shm->layout_hash != topic_shm_layout_hash() || topic_shm_is_retired(shm)) {
    munmap(mapped, sizeof(AgnocastTopicShm));
    return -EINVAL;
  }

  auto wrapper = std::make_unique<TopicWrapper>();
  wrapper->key = key.name;
  wrapper->domain_id = key.domain_id;
  wrapper->shm_fd = shm_fd;
  wrapper->shm = shm;
  closer.fd = -1;
  // Locking also rolls back whatever the previous daemon was doing to the
  // segment if it died holding the mutex.
  topic_shm_lock(shm);
  topic_shm_unlock(shm);
  *out = wrapper.get();
  topic_map_.emplace(key, std::move(wrapper));
  return 0;
}

TopicWrapper * MetadataStore::find_topic(const TopicKey & key) const
{
  auto it = topic_map_.find(key);
  return (it != topic_map_.end()) ? it->second.get() : nullptr;
}

TopicWrapper * MetadataStore::find_or_create_topic_for_process(
  pid_t pid, const std::string & topic_name)
{
  return find_or_create_topic(TopicKey{topic_name, get_process_domain_id(pid)});
}

TopicWrapper * MetadataStore::find_topic_for_process(
  pid_t pid, const std::string & topic_name) const
{
  return find_topic(TopicKey{topic_name, get_process_domain_id(pid)});
}

uint32_t MetadataStore::get_process_domain_id(pid_t pid) const
{
  auto it = proc_info_map_.find(pid);
  return (it != proc_info_map_.end()) ? it->second.domain_id : 0;
}

ProcessInfo * MetadataStore::find_process(pid_t pid)
{
  auto it = proc_info_map_.find(pid);
  return (it != proc_info_map_.end()) ? &it->second : nullptr;
}
