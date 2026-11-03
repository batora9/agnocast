// SPDX-License-Identifier: Apache-2.0
#include "metadata_store.hpp"

#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>

TopicWrapper::~TopicWrapper()
{
  if (shm != nullptr) munmap(shm, sizeof(*shm));
  if (shm_fd >= 0) close(shm_fd);
}

namespace
{

bool create_topic_shm(TopicWrapper & wrapper)
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
  wrapper.shm_fd = fd;
  wrapper.shm = shm;
  return true;
}

}  // namespace

TopicWrapper * MetadataStore::find_or_create_topic(const TopicKey & key)
{
  auto it = topic_map_.find(key);
  if (it != topic_map_.end()) {
    return it->second.get();
  }
  auto wrapper = std::make_unique<TopicWrapper>();
  if (!create_topic_shm(*wrapper)) return nullptr;
  wrapper->key = key.name;
  wrapper->domain_id = key.domain_id;
  auto * ptr = wrapper.get();
  topic_map_.emplace(key, std::move(wrapper));
  return ptr;
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
