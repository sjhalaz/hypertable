/** -*- c++ -*-
 * Copyright (C) 2007-2012 Hypertable, Inc.
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; version 3 of the
 * License, or any later version.
 *
 * Hypertable is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301, USA.
 */

#include "Common/Compat.h"

#include "Context.h"
#include "LoadBalancer.h"
#include "Operation.h"
#include "OperationBalance.h"
#include "RemovalManager.h"
#include "RSRecoveryReplayCounter.h"

using namespace Hypertable;
using namespace std;

Context::~Context() {
  if (hyperspace && master_file_handle > 0) {
    hyperspace->close(master_file_handle);
    master_file_handle = 0;
  }
  delete balancer;
}

void Context::add_server(RangeServerConnectionPtr &rsc) {
  ScopedLock lock(mutex);
  pair<Sequence::iterator, bool> insert_result = m_server_list.push_back( RangeServerConnectionEntry(rsc) );
  if (!insert_result.second) {
    HT_INFOF("Tried to insert %s host=%s local=%s public=%s", rsc->location().c_str(),
             rsc->hostname().c_str(), rsc->local_addr().format().c_str(),
             rsc->public_addr().format().c_str());
    for (Sequence::iterator iter = m_server_list.begin(); iter != m_server_list.end(); ++iter) {
      HT_INFOF("Contains %s host=%s local=%s public=%s", iter->location().c_str(),
               iter->hostname().c_str(), iter->local_addr().format().c_str(),
               iter->public_addr().format().c_str());
    }
    HT_ASSERT(insert_result.second);
  }
}

bool Context::connect_server(RangeServerConnectionPtr &rsc, const String &hostname,
                             InetAddr local_addr, InetAddr public_addr) {
  ScopedLock lock(mutex);
  LocationIndex &hash_index = m_server_list.get<1>();
  LocationIndex::iterator iter;
  PublicAddrIndex &public_addr_index = m_server_list.get<3>();
  PublicAddrIndex::iterator public_addr_iter;


  bool retval = false;
  bool notify = false;

  comm->set_alias(local_addr, public_addr);
  comm->add_proxy(rsc->location(), hostname, public_addr);
  HT_INFOF("Registered proxy %s", rsc->location().c_str());

  if (rsc->connect(hostname, local_addr, public_addr)) {
    conn_count++;
    if (conn_count == 1)
      notify = true;
    retval = true;
  }

  if (m_server_list_iter != m_server_list.end() &&
      m_server_list_iter->location() == rsc->location())
    ++m_server_list_iter;

  // Remove this connection if already exists
  iter = hash_index.find(rsc->location());
  if (iter != hash_index.end())
    hash_index.erase(iter);
  public_addr_iter = public_addr_index.find(rsc->public_addr());
  if (public_addr_iter != public_addr_index.end())
    public_addr_index.erase(public_addr_iter);

  // Add it (or re-add it)
  pair<Sequence::iterator, bool> insert_result = m_server_list.push_back( RangeServerConnectionEntry(rsc) );
  HT_ASSERT(insert_result.second);
  if (m_server_list.size() == 1 || m_server_list_iter == m_server_list.end())
    m_server_list_iter = m_server_list.begin();

  if (notify)
    cond.notify_all();

  return retval;
}

void Context::replay_complete(EventPtr &event) {
  int64_t id;
  uint32_t attempt, fragment;
  map<uint32_t, int> error_map;
  const uint8_t *decode_ptr = event->payload;
  size_t decode_remain = event->payload_len;
  int nn, error;

  id       = Serialization::decode_vi64(&decode_ptr, &decode_remain);
  attempt  = Serialization::decode_vi32(&decode_ptr, &decode_remain);
  nn       = Serialization::decode_vi32(&decode_ptr, &decode_remain);

  HT_DEBUG_OUT << "Received replay_complete for op_id=" << id << " attempt="
      << attempt << " num_ranges=" << nn << " from " << event->proxy << HT_END;

  for (int ii=0; ii<nn; ++ii) {
    fragment = Serialization::decode_vi32(&decode_ptr, &decode_remain);
    error    = Serialization::decode_vi32(&decode_ptr, &decode_remain);
    error_map[fragment] = error;
  }
  {
    ScopedLock lock(m_recovery_mutex);
    RSRecoveryReplayMap::iterator it = m_recovery_replay_map.find(id);
    RSRecoveryReplayCounterPtr replay_counter;
    if (it != m_recovery_replay_map.end()) {
      replay_counter = it->second;
      if (!replay_counter->complete(attempt, error_map)) {
        HT_WARN_OUT << "non-pending player complete message received for operation="
                    << id << " attempt=" << attempt << HT_END;
      }
    }
    else {
      HT_WARN_OUT << "No RSRecoveryReplayCounter found for operation="
                  << id << " attempt=" << attempt << HT_END;
    }
  }

  HT_DEBUG_OUT << "Exitting replay_complete for op_id=" << id << " attempt="
      << attempt << " num_ranges=" << nn << " from " << event->proxy << HT_END;

  return;
}

void Context::erase_rs_recovery_replay_counter(int64_t id) {
  ScopedLock lock(m_recovery_mutex);
  m_recovery_replay_map.erase(id);
}

void Context::install_rs_recovery_replay_counter(int64_t id,
    RSRecoveryReplayCounterPtr &replay_counter) {

  HT_ASSERT(replay_counter != 0);
  ScopedLock lock(m_recovery_mutex);
  if (m_recovery_replay_map.find(id) != m_recovery_replay_map.end()) {
    m_recovery_replay_map.erase(id);
  }
  pair<RSRecoveryReplayMap::iterator, bool> ret;
  ret = m_recovery_replay_map.insert(make_pair(id, replay_counter));
  HT_ASSERT(ret.second);
  return;
}

void Context::prepare_complete(EventPtr &event) {
  const uint8_t *decode_ptr = event->payload;
  size_t decode_remain = event->payload_len;
  int64_t id;
  uint32_t attempt;
  int nn;
  RSRecoveryCounter::Result rr;
  vector<RSRecoveryCounter::Result> results;

  id       = Serialization::decode_vi64(&decode_ptr, &decode_remain);
  attempt  = Serialization::decode_vi32(&decode_ptr, &decode_remain);
  nn       = Serialization::decode_vi32(&decode_ptr, &decode_remain);

  HT_DEBUG_OUT << "Received prepare_complete for op_id=" << id << " attempt="
      << attempt << " num_ranges=" << nn << " from " << event->proxy << HT_END;

  for (int ii=0; ii<nn; ++ii) {
    rr.range.decode(&decode_ptr, &decode_remain);
    rr.error    = Serialization::decode_vi32(&decode_ptr, &decode_remain);
    results.push_back(rr);
  }

  {
    ScopedLock lock(m_recovery_mutex);
    RSRecoveryMap::iterator it = m_recovery_prepare_map.find(id);
    RSRecoveryCounterPtr prepare_counter;
    if (it != m_recovery_prepare_map.end()) {
      prepare_counter = it->second;
      prepare_counter->result_callback(attempt, results);
    }
    else
      HT_WARN_OUT << "No RSRecoveryCounter found for operation=" << id << HT_END;
  }
  HT_DEBUG_OUT << "Exitting prepare_complete for op_id=" << id << " attempt="
      << attempt << " num_ranges=" << nn << " from " << event->proxy << HT_END;

  return;
}

void Context::erase_rs_recovery_prepare_counter(int64_t id) {
  ScopedLock lock(m_recovery_mutex);
  m_recovery_prepare_map.erase(id);
}

void Context::install_rs_recovery_prepare_counter(int64_t id,
    RSRecoveryCounterPtr &prepare_counter) {

  HT_ASSERT(prepare_counter != 0);
  ScopedLock lock(m_recovery_mutex);
  if (m_recovery_prepare_map.find(id) != m_recovery_prepare_map.end())
    m_recovery_prepare_map.erase(id);

  pair<RSRecoveryMap::iterator, bool> ret;
  ret = m_recovery_prepare_map.insert(make_pair(id, prepare_counter));
  HT_ASSERT(ret.second);
  return;
}

void Context::commit_complete(EventPtr &event) {
  const uint8_t *decode_ptr = event->payload;
  size_t decode_remain = event->payload_len;
  int64_t id;
  uint32_t attempt;
  int nn;
  RSRecoveryCounter::Result rr;
  vector<RSRecoveryCounter::Result> results;

  id       = Serialization::decode_vi64(&decode_ptr, &decode_remain);
  attempt  = Serialization::decode_vi32(&decode_ptr, &decode_remain);
  nn       = Serialization::decode_vi32(&decode_ptr, &decode_remain);
  for (int ii=0; ii<nn; ++ii) {
    rr.range.decode(&decode_ptr, &decode_remain);
    rr.error    = Serialization::decode_vi32(&decode_ptr, &decode_remain);
    results.push_back(rr);
  }

  HT_DEBUG_OUT << "Received phantom_commit_complete for op_id=" << id << " attempt="
      << attempt << " num_ranges=" << nn << " from " << event->proxy << HT_END;
  {
    ScopedLock lock(m_recovery_mutex);
    RSRecoveryMap::iterator it = m_recovery_commit_map.find(id);
    RSRecoveryCounterPtr commit_counter;
    if (it != m_recovery_commit_map.end()) {
      commit_counter = it->second;
      commit_counter->result_callback(attempt, results);
    }
    else
      HT_WARN_OUT << "No RSRecoveryCounter found for operation=" << id << HT_END;
  }

  HT_DEBUG_OUT << "Exitting phantom_commit_complete for op_id=" << id << " attempt="
      << attempt << " num_ranges=" << nn << " from " << event->proxy << HT_END;
  return;
}

void Context::erase_rs_recovery_commit_counter(int64_t id) {
  ScopedLock lock(m_recovery_mutex);
  m_recovery_commit_map.erase(id);
}

void Context::install_rs_recovery_commit_counter(int64_t id,
    RSRecoveryCounterPtr &commit_counter) {

  HT_ASSERT(commit_counter != 0);
  ScopedLock lock(m_recovery_mutex);
  if (m_recovery_commit_map.find(id) != m_recovery_commit_map.end())
    m_recovery_commit_map.erase(id);

  pair<RSRecoveryMap::iterator, bool> ret;
  ret = m_recovery_commit_map.insert(make_pair(id, commit_counter));
  HT_ASSERT(ret.second);
  return;
}

bool Context::disconnect_server(RangeServerConnectionPtr &rsc) {
  ScopedLock lock(mutex);
  HT_ASSERT(conn_count > 0);
  if (rsc->disconnect()) {
    conn_count--;
    return true;
  }
  return false;
}

void Context::wait_for_server() {
  ScopedLock lock(mutex);
  while (conn_count == 0)
    cond.wait(lock);
}


bool Context::find_server_by_location(const String &location, RangeServerConnectionPtr &rsc) {
  ScopedLock lock(mutex);
  LocationIndex &hash_index = m_server_list.get<1>();
  LocationIndex::iterator lookup_iter;

  if ((lookup_iter = hash_index.find(location)) == hash_index.end()) {
    rsc = 0;
    return false;
  }
  rsc = lookup_iter->rsc;
  return true;
}


bool Context::find_server_by_hostname(const String &hostname, RangeServerConnectionPtr &rsc) {
  ScopedLock lock(mutex);
  HostnameIndex &hash_index = m_server_list.get<2>();

  pair<HostnameIndex::iterator, HostnameIndex::iterator> p = hash_index.equal_range(hostname);
  if (p.first != p.second) {
    rsc = p.first->rsc;
    if (++p.first == p.second)
      return true;
    rsc = 0;
  }
  return false;
}



bool Context::find_server_by_public_addr(InetAddr addr, RangeServerConnectionPtr &rsc) {
  ScopedLock lock(mutex);
  PublicAddrIndex &hash_index = m_server_list.get<3>();
  PublicAddrIndex::iterator lookup_iter;

  if ((lookup_iter = hash_index.find(addr)) == hash_index.end()) {
    rsc = 0;
    return false;
  }
  rsc = lookup_iter->rsc;
  return true;
}


bool Context::find_server_by_local_addr(InetAddr addr, RangeServerConnectionPtr &rsc) {
  ScopedLock lock(mutex);
  LocalAddrIndex &hash_index = m_server_list.get<4>();

  for (pair<LocalAddrIndex::iterator, LocalAddrIndex::iterator> p = hash_index.equal_range(addr);
       p.first != p.second; ++p.first) {
    if (p.first->connected()) {
      rsc = p.first->rsc;
      return true;
    }
  }
  return false;
}

void Context::erase_server(RangeServerConnectionPtr &rsc) {
  ScopedLock lock(mutex);
  LocationIndex &hash_index = m_server_list.get<1>();
  LocationIndex::iterator iter;
  PublicAddrIndex &public_addr_index = m_server_list.get<3>();
  PublicAddrIndex::iterator public_addr_iter;

  // Remove this connection if already exists
  iter = hash_index.find(rsc->location());
  if (iter != hash_index.end())
    hash_index.erase(iter);
  public_addr_iter = public_addr_index.find(rsc->public_addr());
  if (public_addr_iter != public_addr_index.end())
    public_addr_index.erase(public_addr_iter);
  // reset server list iter
  m_server_list_iter = m_server_list.begin();
}

bool Context::next_available_server(RangeServerConnectionPtr &rsc) {
  ScopedLock lock(mutex);

  if (m_server_list.empty())
    return false;

  if (m_server_list_iter == m_server_list.end())
    m_server_list_iter = m_server_list.begin();

  ServerList::iterator saved_iter = m_server_list_iter;

  do {
    ++m_server_list_iter;
    if (m_server_list_iter == m_server_list.end())
      m_server_list_iter = m_server_list.begin();
    if (m_server_list_iter->rsc->connected()) {
      rsc = m_server_list_iter->rsc;
      return true;
    }
  } while (m_server_list_iter != saved_iter);

  return false;
}

bool Context::reassigned(TableIdentifier *table, RangeSpec &range, String &location) {
  // TBD
  return false;
}


bool Context::is_connected(const String &location) {
  RangeServerConnectionPtr rsc;
  if (find_server_by_location(location, rsc))
    rsc->connected();
  return false;
}


void Context::get_servers(std::vector<RangeServerConnectionPtr> &servers) {
  ScopedLock lock(mutex);
  for (ServerList::iterator iter = m_server_list.begin(); iter != m_server_list.end(); ++iter) {
    if (!iter->removed())
      servers.push_back(iter->rsc);
  }
}

void Context::get_connected_servers(std::vector<RangeServerConnectionPtr> &servers) {
  ScopedLock lock(mutex);
  for (ServerList::iterator iter = m_server_list.begin(); iter != m_server_list.end(); ++iter) {
    if (!iter->removed() && iter->connected())
      servers.push_back(iter->rsc);
  }
}

void Context::get_connected_servers(StringSet &locations) {
  ScopedLock lock(mutex);
  for (ServerList::iterator iter = m_server_list.begin(); iter != m_server_list.end(); ++iter) {
    if (!iter->removed() && iter->connected())
      locations.insert(iter->location());
  }
}

void Context::get_unbalanced_servers(const std::vector<String> &locations,
    std::vector<RangeServerConnectionPtr> &unbalanced) {
  ScopedLock lock(mutex);
  LocationIndex &hash_index = m_server_list.get<1>();
  LocationIndex::iterator lookup_iter;
  RangeServerConnectionPtr rsc;

  foreach(const String &location, locations) {
    if ((lookup_iter = hash_index.find(location)) == hash_index.end())
      continue;
    rsc = lookup_iter->rsc;
    if (!rsc->get_removed() && !rsc->get_balanced())
      unbalanced.push_back(rsc);
  }
}

void Context::set_servers_balanced(const std::vector<RangeServerConnectionPtr> &unbalanced) {
  ScopedLock lock(mutex);
  foreach( const RangeServerConnectionPtr rsc, unbalanced) {
    rsc->set_balanced();
  }
}
