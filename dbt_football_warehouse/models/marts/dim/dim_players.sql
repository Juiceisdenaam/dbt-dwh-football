SELECT
      player_id,
      player_first_name,
      player_last_name,
      player_common_name,
      player_display_name,
      player_full_name,
      player_gender,
      player_height_cm,
      player_weight_kg,
      player_date_of_birth,
      player_city_id,
      player_country_id,
      player_nationality_id,
      player_type_id,
      player_position_id
FROM {{ ref('stg_players') }}