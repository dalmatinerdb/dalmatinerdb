#!/usr/sbin/dtrace -s
/*
 * arg0 arg1 arg2 arg3     arg4
 * PID       4502 UDP Port Count
 *
 * PID -> Erlang process ID. (String)
 * UDP Port - UPD port used.
 * Count - Numbr of messages in badge
 */

/*
 * This function gets called every time a erlang developper probe is
 * fiered, we filter for 801 and 1, so it gets executed when a handler
 * function is entered.
 */

erlang$1:::user_trace*
/ arg2 == 4502 /
{
  /*
   * We cache the relevant strings
   */
  @ = sum(arg4);
}


tick-1s
{
  printa(@);
}
