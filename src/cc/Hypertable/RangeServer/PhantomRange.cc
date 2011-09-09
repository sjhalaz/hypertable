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
#include "Common/md5.h"

#include "PhantomRange.h"

using namespace Hypertable;
using namespace std;

PhantomRange::PhantomRange(const QualifiedRangeStateSpec &range, SchemaPtr &schema,
    const vector<uint32_t> &fragments) : m_spec(range),
    m_schema(schema), m_outstanding(fragments.size()), m_state(INIT) {
      foreach(uint32_t fragment, fragments) {
        FragmentDataPtr data = new FragmentData(fragment);
        m_fragments[fragment] = data;
      }
}

int PhantomRange::get_state() {
  ScopedLock lock(m_mutex);
  return m_state;
}

bool PhantomRange::add(uint32_t fragment, bool more, EventPtr &event) {
  ScopedLock lock(m_mutex);
  FragmentMap::iterator it = m_fragments.find(fragment);
  if (it == m_fragments.end()) {
    String msg = format("Unexpected fragment %u, expected one of ", fragment);
    foreach(const FragmentMap::value_type &vv, m_fragments)
      msg = msg + (String)" " + vv.first;
    HT_FATAL_OUT << msg << HT_END;
  }

  if (it->second->complete()) {
    // fragment is already complete
    return false;
  }
  else {
    HT_ASSERT(m_outstanding);
    if (!more) {
      --m_outstanding;
      if (!m_outstanding)
        m_state = FINISHED_REPLAY;
    }
    it->second->add(more, event);
  }
  return true;
}

void PhantomRange::get_incomplete_fragments(vector<uint32_t> &incomplete_fragments) {
  ScopedLock lock(m_mutex);
  FragmentMap::iterator it = m_fragments.begin();
  for(; it != m_fragments.end(); ++it)
    if (!it->second->complete())
      incomplete_fragments.push_back(it->first);
}

void PhantomRange::purge_incomplete_fragments() {
  ScopedLock lock(m_mutex);
  FragmentMap::iterator it = m_fragments.begin();
  for(; it != m_fragments.end(); ++it)
    if (!it->second->complete())
      it->second->clear();
}

void PhantomRange::create_range(MasterClientPtr &master_client,
  TableInfoPtr &table_info, FilesystemPtr &log_dfs, String &log_dir) {
  ScopedLock lock(m_mutex);

  if (m_state >= RANGE_CREATED)
    HT_WARN_OUT << "Range already created for phantom range " << m_spec << HT_END;
  else {
    m_range = new Range(master_client, &m_spec.qualified_range.table, m_schema,
        &m_spec.qualified_range.range,
        table_info.get(), &m_spec.state, true);
    // replay existing transfer log
    m_range->recovery_finalize();
    m_spec.state.state = m_spec.state.state | RangeState::PHANTOM;
    m_state = RANGE_CREATED;
  }
}

void PhantomRange::populate_range_and_log(FilesystemPtr &log_dfs, const String &log_dir,
    bool *is_empty) {
  ScopedLock lock(m_mutex);
  *is_empty = true;
  if (m_state == RANGE_PREPARED) {
    HT_WARN_OUT << "Range already prepared for phantom range " << m_spec << HT_END;
    return ;
  }

  MetaLog::EntityRange *metalog_entity = m_range->metalog_entity();
  char md5DigestStr[33];
  /**
   * Create phantom log
   */
  md5_trunc_modified_base64(metalog_entity->spec.end_row, md5DigestStr);
  md5DigestStr[16] = 0;
  time_t now=0;

  do {
    if (now != 0)
      poll(0, 0, 1200);
    now = time(0);
    m_phantom_logname = log_dir + "/" + metalog_entity->table.id + "/" +
                        md5DigestStr + "-" + (int)now;
  }
  while (log_dfs->exists(m_phantom_logname));

  log_dfs->mkdirs(m_phantom_logname);

  m_phantom_log = new CommitLog(log_dfs, m_phantom_logname, false);
  size_t table_id_len = m_spec.qualified_range.table.encoded_length();
  DynamicBuffer dbuf(table_id_len);
  int64_t latest_revision;


  Locker<Range> range_lock(*(m_range.get()));
  foreach (FragmentMap::value_type &vv, m_fragments) {
    dbuf.clear();
    m_spec.qualified_range.table.encode(&dbuf.ptr);
    vv.second->merge(m_range, dbuf, &latest_revision);
    if (dbuf.fill() > table_id_len) {
      *is_empty = false;
      m_phantom_log->write(dbuf, latest_revision, false);
    }
  }
  m_phantom_log->close();
  HT_INFO_OUT << "Created phantom log " << m_phantom_logname
      << " for range " << m_spec << HT_END;
  m_state = RANGE_PREPARED;
}

const String & PhantomRange::get_phantom_logname() {
  ScopedLock lock(m_mutex);
  return m_phantom_logname;
}

CommitLogPtr PhantomRange::get_phantom_log() {
  ScopedLock lock(m_mutex);
  return m_phantom_log;
}
