// SPDX-License-Identifier: Apache-2.0
#include "metadata_store.hpp"

TopicWrapper * MetadataStore::find_or_create_topic(const std::string & topic_name)
{
  auto it = topic_map_.find(topic_name);
  if (it != topic_map_.end()) {
    return it->second.get();
  }
  auto wrapper = std::make_unique<TopicWrapper>();
  wrapper->key = topic_name;
  auto * ptr = wrapper.get();
  topic_map_.emplace(topic_name, std::move(wrapper));
  return ptr;
}

TopicWrapper * MetadataStore::find_topic(const std::string & topic_name) const
{
  auto it = topic_map_.find(topic_name);
  return (it != topic_map_.end()) ? it->second.get() : nullptr;
}

ProcessInfo * MetadataStore::find_process(pid_t pid)
{
  auto it = proc_info_map_.find(pid);
  return (it != proc_info_map_.end()) ? &it->second : nullptr;
}
