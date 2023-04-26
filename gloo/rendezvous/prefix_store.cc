/**
 * Copyright (c) 2017-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "prefix_store.h"

#include <sstream>

namespace gloo {
namespace rendezvous {

PrefixStore::PrefixStore(
    const std::string& prefix,
    Store& store)
    : prefix_(prefix), store_(store) {}

std::string PrefixStore::joinKey(const std::string& key) {
  std::stringstream ss;
  ss << prefix_ << "/" << key;
  return ss.str();
}

void PrefixStore::set(const std::string& key, const std::vector<char>& data) {
  store_.set(joinKey(key), data);
}

std::vector<char> PrefixStore::get(const std::string& key) {
  return store_.get(joinKey(key));
}

void PrefixStore::wait(
    const std::vector<std::string>& keys,
    const std::chrono::milliseconds& timeout) {
  std::vector<std::string> joinedKeys;
  joinedKeys.reserve(keys.size());
  for (const auto& key : keys) {
    joinedKeys.push_back(joinKey(key));
  }
  store_.wait(joinedKeys, timeout);
}

void PrefixStore::append(const std::string& key, const std::vector<char>& data) {
  if (!store_.has_append()) {
    GLOO_THROW_INVALID_OPERATION_EXCEPTION("underlying store doesn't support append");
  }
  store_.append(joinKey(key), data);
}

bool PrefixStore::has_append() {
  return store_.has_append();
}


} // namespace rendezvous
} // namespace gloo
