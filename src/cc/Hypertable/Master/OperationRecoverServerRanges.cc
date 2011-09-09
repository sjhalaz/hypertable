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

#include "RSRecoveryReplayCounter.h"
#include "RSRecoveryCounter.h"
#include "OperationRecoverServerRanges.h"

using namespace Hypertable;

OperationRecoverServerRanges::OperationRecoverServerRanges(ContextPtr &context,
    const String &location, int type, vector<QualifiedRangeSpecManaged> &ranges)
  : Operation(context, MetaLog::EntityType::OPERATION_RECOVER_SERVER_RANGES),
  m_location(location), m_type(type), m_attempt(0), m_ranges(ranges) {
  HT_ASSERT(type != RangeSpec::UNKNOWN);
  set_type_str();
  initialize_obstructions_dependencies();
  // XXX SJ TODO: block/wait till all connected servers are "live" ie have recovered
  // + some time so slower servers can connect
}

void OperationRecoverServerRanges::execute() {

  int state = get_state();
  HT_INFOF("Entering RecoverServerRanges %s type=%d attempt=%d state=%s",
           m_location.c_str(), m_type, m_attempt, OperationState::get_text(state));
  bool initial_done=false;
  bool issue_done=false;
  bool prepare_done=false;


  switch (state) {
  case OperationState::INITIAL:
    get_recovery_plan();

    // if there are no fragments or no ranges, there is nothing to do
    if (m_fragments.size()==0 || m_ranges.size()==0) {
      String label = label();
      HT_INFO_OUT << label << " num_fragments=" << m_fragments.size()
          << ", num_ranges=" << m_ranges.size() << " nothing to do, recovery complete"
          << HT_END;
      complete_ok();
      break;
    }

    set_state(OperationState::ISSUE_REQUESTS);
    HT_MAYBE_FAIL(format("recover-server-ranges-%s-INITIAL-a", m_type_str));
    m_context->mml_writer->record_state(this);
    HT_MAYBE_FAIL(format("recover-server-ranges-%s-INITIAL-b", m_type_str));
    initial_done = true;

  case OperationState::ISSUE_REQUESTS:
    // first issue phantom_load requests to destination servers
    // then issue play requests to players.
    // in case any phantom_load request fails, go back to INITIAL state and recreate recovery_plan
    // if requests succeed, then fall through to WAIT_FOR_COMPLETION state
    // the only information to persist at the end of this stage is if we failed to connect to a player
    // that info will be used in the retries state
    if (!initial_done && !validate_recovery_plan()) {
        set_state(OperationState::INITIAL);
        m_context->mml_writer->record_state(this);
        break;
      }
    }
    try {
      if (!replay_commit_log()) {
        // look at failures and modify recovery plan accordingly
        set_state(OperationState::INITIAL);
        m_context->mml_writer->record_state(this);
        break;
      }
    }
    catch (Exception &e) {
      HT_ERROR_OUT << e << HT_END;
      HT_THROW(e.code(), e.what());
    }
    set_state(OperationState::PREPARE);
    m_context->mml_writer->record_state(this);
    issue_done = true;
    // fall through to prepare

  case OperationState::PREPARE:
    if (!issue_done && !validate_recovery_plan()) {
      set_state(OperationState::INITIAL);
      m_context->mml_writer->record_state(this);
      break;
    }
    try {
      // tell destination servers to merge fragment data into range,
      // link in transfer logs to commit log
      if (!prepare_to_commit()) {
        // look at failures and modify recovery plan accordingly
        set_state(OperationState::INITIAL);
        m_context->mml_writer->record_state(this);
        break;
      }
    }
    catch (Exception &e) {
      HT_ERROR_OUT << e << HT_END;
      HT_THROW(e.code(), e.what());
    }
    set_state(OperationState::COMMIT);
    m_context->mml_writer->record_state(this);
    prepare_done = true;
    // fall through to commit
  case OperationState::COMMIT:
    // tell destination servers to update metadata and flip ranges live
    // persist in rsml and mark range as busy
    // finally tell rangeservers to unmark "busy" ranges
    if (!prepare_done && !validate_recovery_plan()) {
      set_state(OperationState::INITIAL);
      m_context->mml_writer->record_state(this);
      break;
    }
    if (!commit()) {
      set_state(OperationState::INITIAL);
      m_context->mml_writer->record_state(this);
    }
    else {
      set_state(OperationState::ACKNOWLEDGE);
      m_context->mml_writer->record_state(this);
      commit_done = true;
    }
  case OperationState::ACKNOWLEDGE:
    if (!commit_done && !validate_recovery_plan()) {
      set_state(OperationState::INITIAL);
      m_context->mml_writer->record_state(this);
      break;
    }
    if (!acknowledge()) {
      set_state(OperationState::INITIAL);
      m_context->mml_writer->record_state(this);
      break;
    }
    complete_ok();

  default:
    HT_FATALF("Unrecognized state %d", state);
    break;
  }

  HT_INFOF("Leaving RecoverServerRanges %s attempt=%d type=%d state=%s",
          m_location.c_str(), m_attempt, m_type, OperationState::get_text(get_state()));
}

void OperationRecoverServerRanges::display_state(std::ostream &os) {
  os << " location=" << m_location << " attempt=" << m_attempt << " type="
     << m_type << " state=%s" << OperationState::get_text(get_state());
}

const String OperationRecoverServerRanges::name() {
  return "OperationRecoverServerRanges";
}

const String OperationRecoverServerRanges::label() {
  return format("RecoverServerRanges %s type=%s", m_location.c_str(), m_type_str);
}

const String OperationRecoverServerRanges::graphviz_label() {
  return label();
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
  size_t len = Serialization::encoded_length_vstr(m_location) + 4 + 4 + 4;
  len += m_plan.encoded_length();
}

void OperationRecoverServerRanges::encode_state(uint8_t **bufp) const {
  Serialization::encode_vstr(bufp, m_location);
  Serialization::encode_i32(bufp, m_type);
  Serialization::encode_i32(bufp, m_attempt);
  m_plan.encode(bufp);
}

void OperationRecoverServerRanges::decode_state(const uint8_t **bufp, size_t *remainp) {
  decode_request(bufp, remainp);
}

void OperationRecoverServerRanges::decode_request(const uint8_t **bufp, size_t *remainp) {
  m_location = Serialization::decode_vstr(bufp, remainp);
  m_type = Serialization::decode_i32(bufp, remainp);
  m_attempt = Serialization::decode_i32(bufp, remainp);
  m_plan.decode(bufp, remainp);
  // read fragments and ranges from m_plan
  m_plan.load_plan.get_ranges(m_ranges);
  m_plan.replay_plan.get_fragments(m_fragments);
  set_type_str();
}

bool OperationRecoverServerRanges::replay_commit_log() {
  // in case of replay failures:
  // master looks at the old plan, reassigns fragments / ranges off the newly failed machines
  // replays the whole plan again from start
  // destination servers keep track of the state of the replay, if they have already received
  // a complete message from a player then they simply inform the player and the player
  // skips over data to that range. players are dumb and store (persist) no state
  // other than in memory plan and map of ranges to skip over
  // state is stored on destination servers (phantom_load state) and master (plan)

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
  bool error=false;
  m_attempt++;
  StringSet locations = m_plan.load_plan.get_locations();
  vector<uint32_t> fragments;
  m_plan.replay_plan.get_fragments(fragments);
  foreach(const String &location, locations) {
    addr.set_proxy(location);
    vector<QualifiedRangeSpec> ranges;
    m_plan.load_plan.get_ranges(location.c_str(), ranges);
    try {
      rsc.phantom_load(addr, location, fragments, ranges);
    }
    catch (Exception &e) {
      error = true;
      HT_ERROR_OUT << e << HT_END;
      break;
    }
  }
  if (error)
    return false;

  // now kick off commit log replay and wait for completion
  RSRecoveryReplayCounterPtr counter = new RSRecoveryReplayCounter(m_attempt);
  m_context->install_rs_recovery_replay_counter(id(), counter);
  error = false;
  StringSet replay_locations;
  m_plan.replay_plan.get_replay_locations(replay_locations);

  uint32_t timeout = rsc.default_timeout();
  foreach(const String &location, replay_locations) {
    try {
      fragments.clear();
      m_plan.replay_plan.get_fragments(location.c_str(), fragments);
      addr.set(location);
      added = false;
      counter->add(fragments.size());
      added = true;
      rsc.play_fragments(addr, id(), m_attempt, m_location, m_type, fragments,
                         m_plan.load_plan, timeout);

    }
    catch (Exception &e) {
      error = true;
      if (added) {
        counter->set_errors(fragments, e.code());
      }
    }
  }

  Timer tt(timeout);
  error |= counter->wait_for_completion(tt);
  m_context->erase_rs_recovery_replay_counter(id());
  // at this point all the players have finished or failed replaying their fragments
  return error;
}

bool OperationRecoverServerRanges::validate_recovery_plan() {

  if (m_plan.type == RangeSpec::UNKNOWN)
    return false;
  StringSet active_locations;
  m_context->get_connected_servers(active_locations);
  HT_ASSERT(active_locations.size()>0);

  // make sure all players are still available
  StringSet players;
  m_plan.replay_plan.get_locations(players);
  foreach(const String &player, players)
    if (active_locations.find(player) == active_locations.end())
      return false;

  // make sure all receivers are still available
  StringSet receivers;
  m_plan.load_plan.get_locations(receivers);
  foreach(const String &receiver, receivers)
    if (active_locations.find(receiver) == active_locations.end())
      return false;

  return true;
}

void OperationRecoverServerRanges::get_recovery_plan() {

  StringSet active_locations;
  m_context->get_connected_servers(active_locations);
  HT_ASSERT(active_locations.size()>0);

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
    m_plan.load_plan.get_locations(receivers);
    foreach(const String &receiver, receivers) {
      if (active_locations.find(receiver) == active_locations.end()) {
        vector<QualifiedRangeSpec> ranges;
        m_plan.load_plan.get_ranges(receiver.c_str(), ranges);
        assign_ranges(ranges, active_locations);
      }
    }
  }
  else {
    if (m_fragments.size()==0)
      read_fragment_ids();
    assign_ranges(m_ranges, active_locations);
    assign_players(m_fragments, active_locations);
  }
}

void OperationRecoverServerRanges::assign_ranges(vector<QualifiedRangeSpec> &ranges,
    const StringSet &locations) {
  const StringSet::iterator location_it = locations.begin();
  // round robin through the locations
  foreach(const QualifiedRangeSpec &range, ranges) {
    if (location_it == locations.end())
      location_it = locations.begin();
    m_plan.load_plan.insert(location_it->c_str(), range.table, range.range);
    ++location_it;
  }
}

void OperationRecoverServerRanges::assign_players(vector<uint32_t> &fragments,
    const StringSet &locations) {
  const StringSet::iterator location_it = locations.begin();
  // round robin through the locations
  foreach(uint32_t fragment, fragments) {
    if (location_it == locations.end())
      location_it = locations.begin();
    m_plan.replay_plan.insert(location_it->c_str(), fragment);
    ++location_it;
  }
}

void OperationRecoverServerRanges::set_type() {
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
  cl_reader->get_fragment_ids(m_fragments);
}

bool OperationRecoverServerRanges::prepare_to_commit() {
  StringSet locations = m_plan.load_plan.get_locations();
  RangeServerClient rsc(m_context->comm);
  CommAddress addr;
  bool error=false;
  uint32_t timeout = rsc.default_timeout();

  RSRecoveryCounter counter = new RSRecoveryCounter(m_context, m_attempt);
  m_context->install_rs_recovery_prepare_counter(id(), counter);

  foreach(const String &location, locations) {
    addr.set_proxy(location);
    vector<QualifiedRangeSpec> ranges;
    m_plan.load_plan.get_ranges(location.c_str(), ranges);

   try {
      counter->add(ranges);
      rsc.phantom_prepare_ranges(addr, id(), m_attempt, m_location, ranges, timeout);
    }
    catch (Exception &e) {
      error = true;
      counter->set_range_errors(ranges, e.code());
      HT_ERROR_OUT << e << HT_END;
    }
  }
  Timer tt(timeout);
  error |= counter->wait_for_completion(tt);
  m_context->erase_rs_recovery_prepare_counter(id());
  // at this point all the players have prepared or failed in creating phantom ranges
  return error;
}

bool OperationRecoverServerRanges::commit() {
  StringSet locations = m_plan.load_plan.get_locations();
  RangeServerClient rsc(m_context->comm);
  CommAddress addr;
  bool error=false;
  uint32_t timeout = rsc.default_timeout();

  RSRecoveryCounter counter = new RSRecoveryCounter(m_context, m_attempt);
  m_context->install_rs_recovery_commit_counter(id(), counter);

  foreach(const String &location, locations) {
    addr.set_proxy(location);
    vector<QualifiedRangeSpec> ranges;
    m_plan.load_plan.get_ranges(location.c_str(), ranges);

   try {
      counter->add(ranges);
      rsc.phantom_commit_ranges(addr, id(), m_attempt, m_location, ranges, timeout);
    }
    catch (Exception &e) {
      error = true;
      counter->set_range_errors(ranges, e.code());
      HT_ERROR_OUT << e << HT_END;
    }
  }
  Timer tt(timeout);
  error |= counter->wait_for_completion(tt);
  m_context->erase_rs_recovery_commit_counter(id());
  // at this point all the players have prepared or failed in creating phantom ranges
  return error;
}

bool OperationRecoverServerRanges::acknowledge() {
  StringSet locations = m_plan.load_plan.get_locations();
  RangeServerClient rsc(m_context->comm);
  CommAddress addr;
  bool error=false;

  foreach(const String &location, locations) {
    addr.set_proxy(location);
    vector<QualifiedRangeSpec*> ranges;
    m_plan.load_plan.get_ranges(location.c_str(), ranges);

   try {
      rsc.acknowledge_load(addr, ranges);
    }
    catch (Exception &e) {
      error = true;
      HT_ERROR_OUT << e << HT_END;
    }
  }
  // at this point all the players have prepared or failed in creating phantom ranges
  return error;
}
