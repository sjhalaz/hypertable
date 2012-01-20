/** -*- c++ -*-
 * Copyright (C) 2011 Hypertable, Inc.
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; version 2 of the
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
#include "Common/Error.h"
#include "Common/md5.h"
#include "Common/FailureInducer.h"

#include "Hypertable/Lib/CommitLogReader.h"

#include "RSRecoveryReplayCounter.h"
#include "RSRecoveryCounter.h"
#include "OperationRecoverServerRanges.h"
#include "OperationRecoveryBlocker.h"
#include "OperationProcessor.h"

using namespace Hypertable;

OperationRecoverServerRanges::OperationRecoverServerRanges(ContextPtr &context,
    const String &location, int type, vector<QualifiedRangeStateSpecManaged> &ranges)
  : Operation(context, MetaLog::EntityType::OPERATION_RECOVER_SERVER_RANGES),
  m_location(location), m_type(type), m_attempt(0), m_ranges(ranges) {
  HT_ASSERT(type != RangeSpec::UNKNOWN);
  set_type_str();
  m_timeout = m_context->props->get_i32("Hypertable.Failover.Timeout");
  m_dependencies.insert(Dependency::RECOVERY_BLOCKER);
  initialize_obstructions_dependencies();
}

OperationRecoverServerRanges::OperationRecoverServerRanges(ContextPtr &context,
    const MetaLog::EntityHeader &header_) : Operation(context, header_) {
  m_dependencies.insert(Dependency::RECOVERY_BLOCKER);
}

void OperationRecoverServerRanges::execute() {

  int state = get_state();
  HT_INFOF("Entering RecoverServerRanges %s type=%d attempt=%d state=%s",
           m_location.c_str(), m_type, m_attempt, OperationState::get_text(state));
  bool initial_done=false;
  bool issue_done=false;
  bool prepare_done=false;
  bool commit_done=false;
  bool blocked = false;

  if (m_timeout == 0)
    m_timeout = m_context->props->get_i32("Hypertable.Failover.Timeout");

  switch (state) {
  case OperationState::INITIAL:
    get_recovery_plan(blocked);
    if (blocked)
      break;

    // if there are no fragments or no ranges, there is nothing to do
    if (m_ranges.size()==0) {
      String label_str = label();
      HT_INFO_OUT << label_str << " num_fragments=" << m_fragments.size()
          << ", num_ranges=" << m_ranges.size() << " nothing to do, recovery complete"
          << HT_END;
      complete_ok();
      break;
    }

    set_state(OperationState::ISSUE_REQUESTS);
    m_context->mml_writer->record_state(this);
    HT_MAYBE_FAIL(format("recover-server-ranges-%s-1", m_type_str.c_str()));
    initial_done = true;

  case OperationState::ISSUE_REQUESTS:
    // first issue phantom_receive requests to destination servers
    // then issue play requests to players.
    // in case any phantom_receive request fails, go back to INITIAL state and recreate recovery_plan
    // if requests succeed, then fall through to WAIT_FOR_COMPLETION state
    // the only information to persist at the end of this stage is if we failed to connect to a player
    // that info will be used in the retries state
    if (!initial_done && !validate_recovery_plan(blocked)) {
      if (blocked)
        break;
      set_state(OperationState::INITIAL);
      m_context->mml_writer->record_state(this);
      HT_MAYBE_FAIL(format("recover-server-ranges-%s-2", m_type_str.c_str()));
      break;
    }
    try {
      if (!replay_commit_log()) {
        // look at failures and modify recovery plan accordingly
        set_state(OperationState::INITIAL);
        m_context->mml_writer->record_state(this);
        HT_MAYBE_FAIL(format("recover-server-ranges-%s-3", m_type_str.c_str()));
        break;
      }
    }
    catch (Exception &e) {
      HT_ERROR_OUT << e << HT_END;
      HT_THROW(e.code(), e.what());
    }
    set_state(OperationState::PREPARE);
    m_context->mml_writer->record_state(this);
    HT_MAYBE_FAIL(format("recover-server-ranges-%s-4", m_type_str.c_str()));
    issue_done = true;
    // fall through to prepare

  case OperationState::PREPARE:
    if (!issue_done && !validate_recovery_plan(blocked)) {
      if (blocked)
        break;
      set_state(OperationState::INITIAL);
      m_context->mml_writer->record_state(this);
      HT_MAYBE_FAIL(format("recover-server-ranges-%s-5", m_type_str.c_str()));
      break;
    }
    try {
      // tell destination servers to merge fragment data into range,
      // link in transfer logs to commit log
      if (!prepare_to_commit()) {
        // look at failures and modify recovery plan accordingly
        set_state(OperationState::INITIAL);
        m_context->mml_writer->record_state(this);
        HT_MAYBE_FAIL(format("recover-server-ranges-%s-6", m_type_str.c_str()));
        break;
      }
    }
    catch (Exception &e) {
      HT_ERROR_OUT << e << HT_END;
      HT_THROW(e.code(), e.what());
    }
    set_state(OperationState::COMMIT);
    m_context->mml_writer->record_state(this);
    HT_MAYBE_FAIL(format("recover-server-ranges-%s-7", m_type_str.c_str()));
    prepare_done = true;
    // fall through to commit
  case OperationState::COMMIT:
    // tell destination servers to update metadata and flip ranges live
    // persist in rsml and mark range as busy
    // finally tell rangeservers to unmark "busy" ranges
    if (!prepare_done && !validate_recovery_plan(blocked)) {
      if (blocked)
        break;
      set_state(OperationState::INITIAL);
      m_context->mml_writer->record_state(this);
      HT_MAYBE_FAIL(format("recover-server-ranges-%s-8", m_type_str.c_str()));
      break;
    }
    if (!commit()) {
      set_state(OperationState::INITIAL);
      m_context->mml_writer->record_state(this);
      HT_MAYBE_FAIL(format("recover-server-ranges-%s-9", m_type_str.c_str()));
    }
    else {
      set_state(OperationState::ACKNOWLEDGE);
      m_context->mml_writer->record_state(this);
      HT_MAYBE_FAIL(format("recover-server-ranges-%s-10", m_type_str.c_str()));
      commit_done = true;
    }
  case OperationState::ACKNOWLEDGE:
    if (!commit_done && !validate_recovery_plan(blocked)) {
      if (blocked)
        break;
      set_state(OperationState::INITIAL);
      m_context->mml_writer->record_state(this);
      HT_MAYBE_FAIL(format("recover-server-ranges-%s-11", m_type_str.c_str()));
      break;
    }
    if (!acknowledge()) {
      set_state(OperationState::INITIAL);
      m_context->mml_writer->record_state(this);
      HT_MAYBE_FAIL(format("recover-server-ranges-%s-12", m_type_str.c_str()));
      break;
    }
    HT_INFOF("RecoverServerRanges complete for server %s attempt=%d type=%d state=%s",
          m_location.c_str(), m_attempt, m_type, OperationState::get_text(get_state()));
    complete_ok();
    HT_MAYBE_FAIL(format("recover-server-ranges-%s-13", m_type_str.c_str()));
    break;

  default:
    HT_FATALF("Unrecognized state %d", state);
    break;
  }

  HT_INFOF("Leaving RecoverServerRanges %s attempt=%d type=%d state=%s",
          m_location.c_str(), m_attempt, m_type, OperationState::get_text(get_state()));
}

void OperationRecoverServerRanges::display_state(std::ostream &os) {
  os << " location=" << m_location << " attempt=" << m_attempt << " type="
     << m_type << " num_ranges=" << m_ranges.size() << " num_fragments=" << m_fragments.size()
     << " recovery_plan type=" << m_plan.type
     << " state=" << OperationState::get_text(get_state());
}

const String OperationRecoverServerRanges::name() {
  return "OperationRecoverServerRanges";
}

const String OperationRecoverServerRanges::label() {
  return format("RecoverServerRanges %s type=%s", m_location.c_str(), m_type_str.c_str());
}

void OperationRecoverServerRanges::initialize_obstructions_dependencies() {
  ScopedLock lock(m_mutex);
  switch(m_type) {
  case RangeSpec::ROOT:
    m_obstructions.insert(Dependency::ROOT);
    break;
  case RangeSpec::METADATA:
    m_obstructions.insert(Dependency::METADATA);
    m_dependencies.insert(Dependency::ROOT);
    break;
  case RangeSpec::SYSTEM:
    m_obstructions.insert(Dependency::SYSTEM);
    m_dependencies.insert(Dependency::ROOT);
    m_dependencies.insert(Dependency::METADATA);
    break;
  case RangeSpec::USER:
    m_obstructions.insert(format("%s-user", m_location.c_str()));
    m_dependencies.insert(Dependency::ROOT);
    m_dependencies.insert(Dependency::METADATA);
    m_dependencies.insert(Dependency::SYSTEM);
    break;
  }
}

size_t OperationRecoverServerRanges::encoded_state_length() const {
  size_t len = Serialization::encoded_length_vstr(m_location) + 4 + 4;
  len += m_plan.encoded_length();
  if (m_plan.type == RangeSpec::UNKNOWN) {
    // recovery plan not populated yet
    len += 4;
    foreach(const QualifiedRangeStateSpecManaged &range, m_ranges)
      len += range.encoded_length();
  }
  return len;
}

void OperationRecoverServerRanges::encode_state(uint8_t **bufp) const {
  Serialization::encode_vstr(bufp, m_location);
  Serialization::encode_i32(bufp, m_type);
  Serialization::encode_i32(bufp, m_attempt);
  m_plan.encode(bufp);
  if (m_plan.type == RangeSpec::UNKNOWN) {
    // recovery plan not populated yet
    Serialization::encode_i32(bufp, m_ranges.size());
    foreach(const QualifiedRangeStateSpecManaged &range, m_ranges)
      range.encode(bufp);
  }
}

void OperationRecoverServerRanges::decode_state(const uint8_t **bufp, size_t *remainp) {
  decode_request(bufp, remainp);
}

void OperationRecoverServerRanges::decode_request(const uint8_t **bufp, size_t *remainp) {
  m_location = Serialization::decode_vstr(bufp, remainp);
  m_type = Serialization::decode_i32(bufp, remainp);
  m_attempt = Serialization::decode_i32(bufp, remainp);
  m_plan.decode(bufp, remainp);
  if (m_plan.type == RangeSpec::UNKNOWN) {
    // recovery plan not populated yet
    uint32_t nn = Serialization::decode_i32(bufp, remainp);
    for (uint32_t ii=0; ii<nn; ++ii) {
      QualifiedRangeStateSpec range;
      range.decode(bufp, remainp);
      m_ranges.push_back(range);
    }
  }
  else {
    // read fragments and ranges from m_plan
    m_plan.receiver_plan.get_ranges(m_ranges);
    m_plan.replay_plan.get_fragments(m_fragments);
  }
  set_type_str();
  m_timeout = 0;
}

bool OperationRecoverServerRanges::replay_commit_log() {
  // in case of replay failures:
  // master looks at the old plan, reassigns fragments / ranges off the newly failed machines
  // replays the whole plan again from start
  // destination servers keep track of the state of the replay, if they have already received
  // a complete message from a player then they simply inform the player and the player
  // skips over data to that range. players are dumb and store (persist) no state
  // other than in memory plan and map of ranges to skip over
  // state is stored on destination servers (phantom_receive state) and master (plan)

  // master kicks off players and waits
  // if players are already in progress (from a previous run) they
  // just return success. when a player completes is calls into some special method
  // (FragmentReplayed )on the
  // master with a recovery id, fragment id, and a list of failed receivers
  // this special method then stores this info and decrements the var on the condition variable
  // the synchronization object can be stored in a map in the context object and shared between
  // the OperationRecoverServerRanges obj and the OperationFragmentReplayed obj

  // first tell destination rangeservers to phatom load ranges

  RangeServerClient rsc(m_context->comm);
  CommAddress addr;
  bool success=true;
  m_attempt++;
  StringSet locations;
  m_plan.receiver_plan.get_locations(locations);
  vector<uint32_t> fragments;
  m_plan.replay_plan.get_fragments(fragments);
  foreach(const String &location, locations) {
    addr.set_proxy(location);
    vector<QualifiedRangeStateSpec> ranges;
    m_plan.receiver_plan.get_ranges(location.c_str(), ranges);
    try {
       HT_INFO_OUT << "Issue phantom_receive for " << ranges.size()
          << " ranges to " << location << HT_END;
      rsc.phantom_receive(addr, m_location, fragments, ranges);
    }
    catch (Exception &e) {
      success = false;
      HT_ERROR_OUT << e << HT_END;
      break;
    }
  }
  if (!success) {
    HT_ERROR_OUT << "Failed to issue phantom_receive calls" << HT_END;
    return success;
  }

  // now kick off commit log replay and wait for completion
  RSRecoveryReplayCounterPtr counter = new RSRecoveryReplayCounter(m_attempt);
  m_context->install_rs_recovery_replay_counter(id(), counter);
  StringSet replay_locations;
  m_plan.replay_plan.get_locations(replay_locations);

  foreach(const String &location, replay_locations) {
    bool added = false;
    try {
      fragments.clear();
      m_plan.replay_plan.get_fragments(location.c_str(), fragments);
      addr.set_proxy(location);
      counter->add(fragments.size());
      added = true;
      HT_INFO_OUT << "Issue replay_fragments for " << fragments.size()
          << " fragments to " << location << HT_END;
      rsc.replay_fragments(addr, id(), m_attempt, m_location, m_type, fragments,
                         m_plan.receiver_plan, m_timeout);

    }
    catch (Exception &e) {
      success = false;
      HT_ERROR_OUT << e << HT_END;
      if (added) {
        counter->set_errors(fragments, e.code());
      }
    }
  }

  Timer tt(m_timeout);
  if (!counter->wait_for_completion(tt)) {
    HT_ERROR_OUT << "Commit log replay failed" << HT_END;
    success = false;
  }
  m_context->erase_rs_recovery_replay_counter(id());
  // at this point all the players have finished or failed replaying their fragments
  return success;
}

bool OperationRecoverServerRanges::validate_recovery_plan(bool &blocked) {

  blocked = false;
  if (m_plan.type == RangeSpec::UNKNOWN)
    return false;
  StringSet active_locations;
  m_context->get_connected_servers(active_locations);
  size_t total_servers = m_context->connected_server_count();
  size_t quorum = (total_servers * m_context->props->get_i32("Hypertable.Failover.Quorum.Percentage") )/100;
  if (active_locations.size() < quorum || active_locations.size() == 0) {
    // wait for at least half the servers to be up before proceeding
    HT_INFO_OUT << "Only " << active_locations.size() << " servers ready, total servers="
        << total_servers << " quorum=" << quorum << ", wait for servers" << HT_END;
    OperationPtr op = new OperationRecoveryBlocker(m_context);
    try {
      m_context->op->add_operation(op);
    }
    catch (Exception &e) {
      HT_ERROR_OUT << e << HT_END;
    }
    blocked = true;
    return false;
  }

  HT_ASSERT(active_locations.size()>0);

  // make sure all players are still available
  StringSet players;
  m_plan.replay_plan.get_locations(players);
  foreach(const String &player, players)
    if (active_locations.find(player) == active_locations.end())
      return false;

  // make sure all receivers are still available
  StringSet receivers;
  m_plan.receiver_plan.get_locations(receivers);
  foreach(const String &receiver, receivers)
    if (active_locations.find(receiver) == active_locations.end())
      return false;

  return true;
}

void OperationRecoverServerRanges::get_recovery_plan(bool &blocked) {

  blocked = false;
  StringSet active_locations;
  m_context->get_connected_servers(active_locations);
  size_t total_servers = m_context->connected_server_count();
  size_t quorum = (total_servers * m_context->props->get_i32("Hypertable.Failover.Quorum.Percentage") )/100;

  if (active_locations.size() < quorum || active_locations.size() == 0) {
    blocked = true;
    // wait for at least half the servers to be up before proceeding
    HT_INFO_OUT << "Only " << active_locations.size() << " servers ready, total servers="
        << total_servers << " quorum=" << quorum << ", wait for servers" << HT_END;

    OperationPtr op = new OperationRecoveryBlocker(m_context);
    try {
      m_context->op->add_operation(op);
    }
    catch (Exception &e) {
      HT_ERROR_OUT << e << HT_END;
    }
    return;
  }

  if (m_plan.type != RangeSpec::UNKNOWN) {
    // modify existing plan
    // iterate through players and reassign any fragments that are on an inactive server
    StringSet players;
    m_plan.replay_plan.get_locations(players);
    foreach(const String &player, players) {
      if (active_locations.find(player) == active_locations.end()) {
        vector<uint32_t> fragments;
        m_plan.replay_plan.get_fragments(player.c_str(), fragments);
        assign_players(fragments, active_locations);
      }
    }
    // iterate through receivers and reassign any ranges that are on an inactive server
    StringSet receivers;
    m_plan.receiver_plan.get_locations(receivers);
    foreach(const String &receiver, receivers) {
      if (active_locations.find(receiver) == active_locations.end()) {
        vector<QualifiedRangeStateSpec> ranges;
        m_plan.receiver_plan.get_ranges(receiver.c_str(), ranges);
        assign_ranges(ranges, active_locations);
      }
    }
  }
  else {
    if (m_fragments.size()==0)
      read_fragment_ids();
    assign_ranges(m_ranges, active_locations);
    assign_players(m_fragments, active_locations);
    m_plan.type = m_type;
  }
}

void OperationRecoverServerRanges::assign_ranges(const vector<QualifiedRangeStateSpec> &ranges,
    const StringSet &locations) {
  StringSet::const_iterator location_it = locations.begin();
  // round robin through the locations
  foreach(const QualifiedRangeStateSpec &range, ranges) {
    if (location_it == locations.end())
      location_it = locations.begin();
    m_plan.receiver_plan.insert(location_it->c_str(), range.qualified_range.table,
        range.qualified_range.range, range.state);
    ++location_it;
  }
}

void OperationRecoverServerRanges::assign_ranges(const vector<QualifiedRangeStateSpecManaged> &ranges,
    const StringSet &locations) {
  StringSet::const_iterator location_it = locations.begin();
  // round robin through the locations
  foreach(const QualifiedRangeStateSpec &range, ranges) {
    if (location_it == locations.end())
      location_it = locations.begin();
    m_plan.receiver_plan.insert(location_it->c_str(), range.qualified_range.table,
        range.qualified_range.range, range.state);
    ++location_it;
  }
}

void OperationRecoverServerRanges::assign_players(const vector<uint32_t> &fragments,
    const StringSet &locations) {
  StringSet::const_iterator location_it = locations.begin();
  // round robin through the locations
  foreach(uint32_t fragment, fragments) {
    if (location_it == locations.end())
      location_it = locations.begin();
    m_plan.replay_plan.insert(location_it->c_str(), fragment);
    ++location_it;
  }
}

void OperationRecoverServerRanges::set_type_str() {
  switch(m_type) {
    case RangeSpec::ROOT:
      m_type_str = "root";
      break;
    case RangeSpec::METADATA:
      m_type_str = "metadata";
      break;
    case RangeSpec::SYSTEM:
      m_type_str = "system";
      break;
    case RangeSpec::USER:
      m_type_str = "user";
      break;
    default:
      m_type_str = "UNKNOWN";
  }
}

void OperationRecoverServerRanges::read_fragment_ids() {
  String log_dir = m_context->toplevel_dir + "/servers/" + m_location + "/log/" + m_type_str;
  CommitLogReader cl_reader(m_context->dfs, log_dir);

  m_fragments.clear();
  cl_reader.get_init_fragment_ids(m_fragments);
  //foreach(uint32_t fragment, m_fragments) {
  //  HT_DEBUG_OUT << "Found fragment " << fragment << " in log " << log_dir << HT_END;
  //}
}

bool OperationRecoverServerRanges::prepare_to_commit() {
  StringSet locations;
  RangeServerClient rsc(m_context->comm);
  CommAddress addr;
  bool success = true;

  m_plan.receiver_plan.get_locations(locations);
  RSRecoveryCounterPtr counter = new RSRecoveryCounter(m_attempt);
  m_context->install_rs_recovery_prepare_counter(id(), counter);

  foreach(const String &location, locations) {
    addr.set_proxy(location);
    vector<QualifiedRangeSpec> ranges;
    m_plan.receiver_plan.get_ranges(location.c_str(), ranges);

    HT_INFO_OUT << "Issue phantom_prepare_ranges for " << ranges.size()
        << " ranges to " << location << HT_END;
    try {
      counter->add(ranges);
      rsc.phantom_prepare_ranges(addr, id(), m_attempt, m_location, ranges, m_timeout);
    }
    catch (Exception &e) {
      success = false;
      counter->set_range_errors(ranges, e.code());
      HT_ERROR_OUT << e << HT_END;
    }
  }
  Timer tt(m_timeout);
  if (!counter->wait_for_completion(tt))
    success = false;
  m_context->erase_rs_recovery_prepare_counter(id());
  // at this point all the players have prepared or failed in creating phantom ranges
  return success;
}

bool OperationRecoverServerRanges::commit() {
  StringSet locations;
  RangeServerClient rsc(m_context->comm);
  CommAddress addr;
  bool success = true;

  m_plan.receiver_plan.get_locations(locations);
  RSRecoveryCounterPtr counter = new RSRecoveryCounter(m_attempt);
  m_context->install_rs_recovery_commit_counter(id(), counter);

  foreach(const String &location, locations) {
    addr.set_proxy(location);
    vector<QualifiedRangeSpec> ranges;
    m_plan.receiver_plan.get_ranges(location.c_str(), ranges);

   try {
      counter->add(ranges);
      HT_INFO_OUT << "Issue phantom_commit_ranges for " << ranges.size()
          << " ranges to " << location << HT_END;
      rsc.phantom_commit_ranges(addr, id(), m_attempt, m_location, ranges, m_timeout);
    }
    catch (Exception &e) {
      success = false;
      counter->set_range_errors(ranges, e.code());
      HT_ERROR_OUT << e << HT_END;
    }
  }
  Timer tt(m_timeout);
  if (!counter->wait_for_completion(tt))
    success = false;
  m_context->erase_rs_recovery_commit_counter(id());
  // at this point all the players have prepared or failed in creating phantom ranges
  return success;
}

bool OperationRecoverServerRanges::acknowledge() {
  StringSet locations;
  RangeServerClient rsc(m_context->comm);
  CommAddress addr;
  bool success = true;
  m_plan.receiver_plan.get_locations(locations);

  foreach(const String &location, locations) {
    addr.set_proxy(location);
    vector<QualifiedRangeSpec> ranges;
    vector<QualifiedRangeSpec *> range_ptrs;
    map<QualifiedRangeSpec, int> response_map;
    map<QualifiedRangeSpec, int>::iterator response_map_it;

    m_plan.receiver_plan.get_ranges(location.c_str(), ranges);
    foreach(QualifiedRangeSpec &range, ranges)
      range_ptrs.push_back(&range);
    try {
      HT_INFO_OUT << "Issue acknowledge_load for " << range_ptrs.size()
          << " ranges to " << location << HT_END;
      rsc.acknowledge_load(addr, range_ptrs, response_map);
      response_map_it = response_map.begin();
      while(response_map_it != response_map.end()) {
        if (response_map_it->second != Error::OK)
          HT_THROW(response_map_it->second, (String)"Error acknowledging load for " +
              response_map_it->first.table.id + "[" +
              response_map_it->first.range.start_row + ".." +
              response_map_it->first.range.end_row + "]");
        ++response_map_it;
      }
      HT_INFO_OUT << "acknowledge_load complete for " << range_ptrs.size()
          << " ranges to " << location << HT_END;
    }
    catch (Exception &e) {
      success = false;
      HT_ERROR_OUT << e << HT_END;
    }
  }
  // at this point all the players have prepared or failed in creating phantom ranges
  return success;
}
