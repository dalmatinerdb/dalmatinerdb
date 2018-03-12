-record(read_req, {
          mstore      :: mstore:mstore(),
          metric      :: binary(),
          time        :: non_neg_integer(),
          count       :: pos_integer(),
          compression :: snappy | none,
          map_fn      :: undefined | fun((binary()) -> binary()),
          req_id      :: term(),
          hpts        :: boolean()
         }).
