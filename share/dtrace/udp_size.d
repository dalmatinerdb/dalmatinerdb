#!/usr/sbin/dtrace -s
/*
 * arg0 arg1 arg2 arg3     arg4 
 * PID       4502 UDP Port Bytes
 *
 * PID -> Erlang process ID. (String)
 * UDP Port - UPD port used.
 * Bytes - Bytes received.
 */

/*
 * Sums up the total bytes recived.
 */

erlang$1:::user_trace*
/ arg2 == 4501 /
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
