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

#ifndef HYPERTABLE_RSRECOVERYREPLAYCOUNTER_H
#define HYPERTABLE_RSRECOVERYREPLAYCOUNTER_H

#include "Common/ReferenceCount.h"
#include "Common/Time.h"

namespace Hypertable {

  /**
   * Tracks outstanding RangeServer recover requests.
   */
  class RSRecoveryReplayCounter : public ReferenceCount {
  public:
    typedef map<uint32_t, int> ErrorMap;

    RSRecoveryReplayCounter(uint32_t attempt)
      : m_attempt(attempt), m_outstanding(0), m_done(false), m_errors(false),
        m_timed_out(false) { }

    void add(size_t num) {
      ScopedLock lock(m_mutex);
      m_outstanding+= num;
    }

    bool complete(uint32_t attempt, const ErrorMap &error_map) {
      ScopedLock lock(m_mutex);
      HT_ASSERT(m_outstanding >= error_map.size());
      m_outstanding -= error_map.size();

      if (attempt != m_attempt)
        return false;

      foreach(const ErrorMap::value_type &vv, error_map) {
        m_error_map[vv.first] = vv.second;
        if (vv.second != Error::OK)
          m_errors = true;
      }
      if (m_outstanding == 0) {
        m_done = true;
        m_cond.notify_all();
      }
      return true;
    }

    bool wait_for_completion(Timer &timer) {
      ScopedLock lock(m_mutex);
      boost::xtime expire_time;

      timer.start();

      while (m_outstanding) {
        boost::xtime_get(&expire_time, boost::TIME_UTC);
        xtime_add_millis(expire_time, timer.remaining());
        if (!m_cond.timed_wait(lock, expire_time)) {
          HT_INFO_OUT << "RSRecoveryReplayCounter timed out" << HT_END;
          m_errors = true;
          m_timed_out = true;
        }
      }
      m_done = true;
      return m_errors;
    }

    void set_errors(const vector<uint32_t> &fragments, int error) {
      ScopedLock lock(m_mutex);
      m_outstanding -= fragments.size();
      foreach(uint32_t fragment, fragments) {
        m_error_map[fragment] = error;
      }
      if (m_outstanding == 0) {
        m_done = true;
        m_cond.notify_all();
      }
    }

    const ErrorMap &get_errors() {
      ScopedLock lock(m_mutex);
      HT_ASSERT(m_done);
      return m_error_map;
    }

    bool timed_out() {
      ScopedLock lock(m_mutex);
      return m_timed_out;
    }

    uint32_t get_attempt() const { return m_attempt; }

  protected:
    uint32_t m_attempt;
    Mutex m_mutex;
    boost::condition m_cond;
    size_t m_outstanding;
    bool m_errors;
    bool m_done;
    bool m_timed_out;
    ErrorMap m_error_map;
  };
  typedef intrusive_ptr<RSRecoveryReplayCounter> RSRecoveryReplayCounterPtr;
}

#endif // HYPERTABLE_RSRECOVERYREPLAYCOUNTER_H
