/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

#pragma once

#include <cstdlib>
#include <memory>
#include <string_view>
#include <utility>

#include "event2/buffer.h"
#include "event2/bufferevent.h"
#include "event2/event.h"
#include "event2/listener.h"
#include "type_util.h"

using StaticFree = StaticFunction<decltype(std::free), std::free>;

template <typename T>
struct UniqueFreePtr : std::unique_ptr<T, StaticFree> {
  using BaseType = std::unique_ptr<T, StaticFree>;

  using BaseType::BaseType;
};

struct UniqueEvbufReadln : UniqueFreePtr<char[]> {
  UniqueEvbufReadln(evbuffer *buffer, evbuffer_eol_style eol_style)
      : UniqueFreePtr(evbuffer_readln(buffer, &length, eol_style)) {}

  size_t length;

  std::string_view View() { return {get(), length}; }
};

using StaticEvbufFree = StaticFunction<decltype(evbuffer_free), evbuffer_free>;

struct UniqueEvbuf : std::unique_ptr<evbuffer, StaticEvbufFree> {
  using BaseType = std::unique_ptr<evbuffer, StaticEvbufFree>;

  UniqueEvbuf() : BaseType(evbuffer_new()) {}
  explicit UniqueEvbuf(evbuffer *buffer) : BaseType(buffer) {}
};

using StaticEventFree = StaticFunction<decltype(event_free), event_free>;

struct UniqueEvent : std::unique_ptr<event, StaticEventFree> {
  using BaseType = std::unique_ptr<event, StaticEventFree>;

  UniqueEvent() : BaseType(nullptr) {}
  explicit UniqueEvent(event *buffer) : BaseType(buffer) {}
};

template <typename Derived, bool ReadCB = true, bool WriteCB = true, bool EventCB = true>
struct EvbufCallbackBase {
 private:
  static void readCB(bufferevent *bev, void *ctx) { static_cast<Derived *>(ctx)->OnRead(bev); }

  static void writeCB(bufferevent *bev, void *ctx) { static_cast<Derived *>(ctx)->OnWrite(bev); }

  static void eventCB(bufferevent *bev, short what, void *ctx) { static_cast<Derived *>(ctx)->OnEvent(bev, what); }

  static auto getReadCB() {
    if constexpr (ReadCB) {
      return readCB;
    } else {
      return nullptr;
    }
  }
  static auto getWriteCB() {
    if constexpr (WriteCB) {
      return writeCB;
    } else {
      return nullptr;
    }
  }

  static auto getEventCB() {
    if constexpr (EventCB) {
      return eventCB;
    } else {
      return nullptr;
    }
  }

 public:
  void SetCB(bufferevent *bev) {
    bufferevent_setcb(bev, getReadCB(), getWriteCB(), getEventCB(), reinterpret_cast<void *>(this));
  }
};

template <typename Derived>
struct EventCallbackBase {
 private:
  static void timerCB(evutil_socket_t fd, short events, void *ctx) { static_cast<Derived *>(ctx)->TimerCB(fd, events); }

 public:
  event *NewEvent(event_base *base, evutil_socket_t fd, short events) {
    return event_new(base, fd, events, timerCB, reinterpret_cast<void *>(this));
  }

  event *NewTimer(event_base *base) { return evtimer_new(base, timerCB, reinterpret_cast<void *>(this)); }
};

template <typename Derived>
struct EvconnlistenerBase {
 private:
  template <void (Derived::*cb)(evconnlistener *, evutil_socket_t, sockaddr *, int)>
  static void callback(evconnlistener *listener, evutil_socket_t fd, sockaddr *address, int socklen, void *ctx) {
    return (reinterpret_cast<Derived *>(ctx)->*cb)(listener, fd, address, socklen);
  }

 public:
  template <void (Derived::*cb)(evconnlistener *, evutil_socket_t, sockaddr *, int)>
  evconnlistener *NewEvconnlistener(event_base *base, unsigned flags, int backlog, evutil_socket_t fd) {
    return evconnlistener_new(base, callback<cb>, this, flags, backlog, fd);
  }
};

namespace details {

template <auto F, typename>
struct EventCallbackImpl;

template <auto F, typename T, typename R, typename... Args>
struct EventCallbackImpl<F, R (T::*)(Args...)> {
  static R Func(Args... args, void *ctx) { return (reinterpret_cast<T *>(ctx)->*F)(args...); }
};

}  // namespace details

// convert member functions to eventbuffer callbacks
// e.g. for member function `void A::f(int x)` from class A
// EventCallback<&A::f> generate a function
// void EventCallback<&A::f>::Func(int x, void *ctx)
// and put `this` pointer of A to `void *ctx`
template <auto F>
struct EventCallback : details::EventCallbackImpl<F, decltype(F)> {};

template <auto F>
constexpr auto EventCallbackFunc = EventCallback<F>::Func;
