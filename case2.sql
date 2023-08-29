{{
    config(
        materialized='view',
        tags=['adjusted']
    )
}}

    select
        travel_month
      , point_of_sale_country
      , point_of_origin_airport
      , scheduled_departure_airport_iata
      , scheduled_arrival_airport_iata
      , dominant_owner_carrier
      , dominant_marketing_carrier
      , dominant_operating_carrier
      , marketing_carrier_leg_1
      , marketing_carrier_leg_2
      , marketing_carrier_leg_3
      , operating_carrier_leg_1
      , operating_carrier_leg_2
      , operating_carrier_leg_3
      , connecting_airport_1
      , connecting_airport_2
      , connecting_airport_1_a
      , connecting_airport_2_a
      , stopping_airport_1_leg_1
      , stopping_airport_2_leg_1
      , stopping_airport_3_leg_1
      , stopping_airport_1_leg_2
      , stopping_airport_2_leg_2
      , stopping_airport_3_leg_2
      , stopping_airport_1_leg_3
      , stopping_airport_2_leg_3
      , stopping_airport_3_leg_3
      , passengers_booking_class_a
      , passengers_booking_class_b
      , passengers_booking_class_c
      , passengers_booking_class_d
      , passengers_booking_class_e
      , passengers_booking_class_f
      , passengers_booking_class_g
      , passengers_booking_class_h
      , passengers_booking_class_i
      , passengers_booking_class_j
      , passengers_booking_class_k
      , passengers_booking_class_l
      , passengers_booking_class_m
      , passengers_booking_class_n
      , passengers_booking_class_o
      , passengers_booking_class_p
      , passengers_booking_class_q
      , passengers_booking_class_r
      , passengers_booking_class_s
      , passengers_booking_class_t
      , passengers_booking_class_u
      , passengers_booking_class_v
      , passengers_booking_class_w
      , passengers_booking_class_x
      , passengers_booking_class_y
      , passengers_booking_class_z
      , passengers_booking_class_unknown
      , passengers_total
      , traffic_id
    from {{ ref('ephemeral_adjusted_active') }}
