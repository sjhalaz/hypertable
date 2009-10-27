/**
 * Copyright (C) 2009 Sanjit Jhala(Zvents, Inc.)
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or any later version.
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

#ifndef HYPERSPACE_REQUESTCOMPLETIONTRACKER_H
#define HYPERSPACE_REQUESTCOMPLETIONTRACKER_H

#include "Common/Compat.h"

extern "C" {
#include <poll.h>
}

#include <iostream>
#include <string>

#include <boost/thread/condition.hpp>

#include "Common/Mutex.h"
#include "Common/ReferenceCount.h"
#include "Common/Serialization.h"
#include "Common/System.h"

#include "AsyncComm/CommBuf.h"

namespace Hyperspace {
  using namespace Hypertable;

  class RequestCompletionTracker: public ReferenceCount {
  public:
    RequestCompletionTracker() : m_complete(false) {
    }

    virtual ~RequestCompletionTracker() { return; }

    void complete() {
      ScopedLock lock(m_mutex);
      if (m_complete == false) {
        m_complete = true;
        m_cond.notify_all();
      }
    }

    void wait_for_completion() {
      ScopedLock lock(m_mutex);
      if (!m_complete)
        m_cond.wait(lock);
    }

  protected:
    Mutex             m_mutex;
    boost::condition  m_cond;
    bool              m_complete;
  };

  typedef boost::intrusive_ptr<RequestCompletionTracker> HyperspaceRequestCompletionTrackerPtr;

} // namespace Hyperspace

#endif // HYPERSPACE_REQUESTRECOVERYTRACKER_H
