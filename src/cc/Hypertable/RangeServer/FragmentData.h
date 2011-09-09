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

#ifndef HYPERTABLE_FRAGMENTDATA_H
#define HYPERTABLE_FRAGMENTDATA_H

#include <vector>
#include "Common/Mutex.h"
#include "Common/atomic.h"
#include "Common/ByteString.h"
#include "Common/ReferenceCount.h"

#include "ScanContext.h"

#include "Hypertable/Lib/Key.h"

namespace Hypertable {


  /**
   * Stores fragment data for a specific range
   */
  class FragmentData {
  public:
    FragmentData() : m_done(false) {}
    ~FragmentData() {}

    /**
     * Adds EventPtr to data for this fragment
     *
     * @param more if false then this fragment is complete
     * @param event event_ptr
     * @return true if there is no more data to be added for this fragment
     */
    void add(bool more, EventPtr &event);
    bool complete() const { return m_done; }
    void clear() {
      HT_ASSERT(!m_done);
      m_data.clear();
    }

    /**
     * write the contents of this fragment into the Range and the dynamic buffer
     */
    void merge(RangePtr &range, DynamicBuffer &dbuf, int64_t *latest_revision);

  protected:
    uint32_t m_id;
    bool m_done;
    vector<EventPtr> m_data;
  };
  typedef boost::intrusive_ptr<FragmentData> FragmentDataPtr;

}

#endif // HYPERTABLE_FRAGMENTDATA_H
