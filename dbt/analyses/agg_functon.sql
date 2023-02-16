select 
     city_name
    ,percent_congestion
from 
    {{source("raw_airport_city","city_congestion")}}
where
    percent_congestion = (select 
                                max(percent_congestion)
                        from 
                            {{source("raw_airport_city","city_congestion")}}
                        )
