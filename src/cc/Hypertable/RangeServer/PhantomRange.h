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

#ifndef HYPERTABLE_PHANTOMRANGE_H
#define HYPERTABLE_PHANTOMRANGE_H

#include <map>
#include <vector>

#include "Common/String.h"
#include "Common/Filesystem.h"
#include "Common/ReferenceCount.h"

#include "Hypertable/Lib/Types.h"

#include "Range.h"
#include "TableInfo.h"
#include "FragmentData.h"

namespace Hypertable {
  using namespace std;
  /**
   * Represents a table row range.
   */
  class PhantomRange : public ReferenceCount {

  public:
    enum State {
      INIT=0,
      FINISHED_REPLAY=1,
      RANGE_CREATED=2,
      RANGE_PREPARED=3
    };

    PhantomRange(const QualifiedRangeStateSpec &range, SchemaPtr &schema,
                 const vector<uint32_t> &fragments);
    ~PhantomRange() {}
    /**
     *
     * @param fragment fragment id
     * @param more if false this is the last data for this fragment
     * @param empty if true then there is no data for this range
     * @param event contains data fort his fragment
     * @return true if the add succeded, false means the fragment is already complete
     */
    bool add(uint32_t fragment, bool more, EventPtr &event);
    int get_state();
    const QualifiedRangeSpec &get_qualified_range_spec() { return m_spec.qualified_range; }
    const RangeState &get_range_state() { return m_spec.state; }
    const QualifiedRangeStateSpec &get_qualified_range_state_spec() { return m_spec; }

    void purge_incomplete_fragments();
    void get_incomplete_fragments(vector<uint32_t> &fragments);
    void create_range(MasterClientPtr &master_client, TableInfoPtr &table_info,
                      FilesystemPtr &log_dfs, String &log_dir);
    RangePtr& get_range() {
      ScopedLock lock(m_mutex);
      return m_range;
    }

    void populate_range_and_log(FilesystemPtr &log_dfs, const String &log_dir, bool *is_empty);
    CommitLogPtr get_phantom_log();
    const String &get_phantom_logname();

  private:
    typedef std::map<uint32_t, FragmentDataPtr> FragmentMap;
    Mutex            m_mutex;
    FragmentMap      m_fragments;
    QualifiedRangeStateSpecManaged m_spec;
    SchemaPtr        m_schema;
    size_t           m_outstanding;
    RangePtr         m_range;
    CommitLogPtr     m_phantom_log;
    String           m_phantom_logname;
    int              m_state;
  };

  typedef intrusive_ptr<PhantomRange> PhantomRangePtr;

} // namespace Hypertable

#endif // HYPERTABLE_PHANTOMRANGE_H
