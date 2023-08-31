{
  "namespace": "rtb.gateway.forecast_sampled",
  "type": "record",
  "name": "ForecastSampled",
  "doc":  "data for forecasting Barker app",
  "fields": [
    {
      "name": "bidder_ids",
      "type": {
          "type": "array",
          "items": "long"
      },
      "doc": "Bidder is list for flight matching"
    },
    {"name": "exchange", "type": "long", "doc": "Exchange Id"},
    {"name": "member_id","type": "long", "doc": "Exchange Seller Id"},
    {
      "name": "country",
      "type": ["null", "string"],
      "doc": "Country 2 letter code"
    },
    {
      "name": "region",
      "type": ["null", "string"],
      "doc": "Region or state"
    },
    {
      "name": "city",
      "type": ["null", "string"],
      "doc": "City name"
    },
    {
      "name": "pc",
      "type": ["null", "string"],
      "doc": "Postal/zip code"
    },
    {"name": "dma","type": ["null", "long"], "doc": "DMA code (Designated Market Area)"},
    {"name": "lg","type": ["null", "string"], "doc": "Language code ISO 639-1"},
    {
      "name": "doubleclick_verticals",
      "type": ["null", {"type":"array", "items":"long"}],
      "doc": "??????"
    },
    {
      "name": "iab_categories",
      "type": ["null", {
          "type": "array",
          "items": "long"
      }],
      "doc": "list of IAB categories for request"
    },
    {
      "name": "device_type_id",
      "type": ["null", "long"],
      "doc": "OpenRTB device type id"
    },
    {
      "name": "device_id",
      "type": ["string", "long"],
      "doc": "Device Id from request"
    },
    {
      "name": "operating_system_id",
      "type": ["null", "long"],
      "doc": "System ID from erl_adgear_ua:classify(UserAgent)"
    },
    {
      "name": "operating_system_variant_id",
      "type": ["null", "long"],
      "doc": "System Varian ID from erl_adgear_ua:classify(UserAgent)"
    },
    {
      "name": "browser_id",
      "type": ["null", "long"],
      "doc": "Browser ID from erl_adgear_ua:classify(UserAgent)"
    },
    {"name": "browser_variant_id","type": "long", "doc": "from erl_adgear_ua:classify(UserAgent)"},
    {
      "name": "matched_user",
      "type": ["null","boolean"],
      "doc": "BuyerId /= undefined andalso not UidNew) orelse Ifa /= undefined"
    },
    {
      "name": "matched_tv",
      "type": ["null","boolean"],
      "doc": "Is matched TV by boolean expression"
    },
    {
      "name": "matched_ip",
      "type": ["null","boolean"],
      "doc": "Is ip matched by boolean expression"
    },
    {
      "name": "matched_idl_pid",
      "type": ["null","boolean"],
      "doc": "Is idl_pid matched by boolean expression"
    },
    {
      "name": "matched_idl_hhid",
      "type": ["null","boolean"],
      "doc": "Is idl_hhid matched by boolean expression"
    },
    {
      "name": "latitude",
      "type": ["float", "null"],
      "doc": "Latitude from -90.0 to +90.0, where negative is south"
    },
    {
      "name": "longitude",
      "type": ["float", "null"],
      "doc": "Longitude from -180.0 to +180.0, where negative is west"
    },
    {
      "name": "device_located",
      "type": ["null","boolean"],
      "doc": "Is matched by boolean expression"
    },
    {
      "name": "frequency_caps",
      "type": ["null", {
          "type": "array",
          "items":[
            {
              "type": "array",
              "items": ["long", {
                "type": "array",
                "items": ["long", "string"]
              }]
            }
          ]
      }],
      "doc": "Frequency Cap metadata"
    },
    {
      "name": "sitelist_ids",
      "type": {
          "type": "array",
          "items": "long"
      },
      "doc": "List of site ids from boolena expression"
    },
    {
      "name": "adgear_segments",
      "type": ["null", {
          "type": "array",
          "items": "long"
      }],
      "doc": "AdGear segments from boolean expressions"
    },
    {
      "name": "segments_with_timestamp",
      "type": ["null", {
          "type": "array",
          "items": {
            "type": "array",
            "items": "long"
          }
      }],
      "doc": "????"
    },
    {"name": "ip","type": ["null","string"], "doc": "IP address from bid request"},
    {"name": "now","type": "long", "doc": "trunc(rtb_lib_utils:timestamp_to_epoch(Timestamp))"},
    {
      "name": "iplist_ids",
      "type": ["null", {
          "type": "array",
          "items": "long"
      }],
      "doc": "iplist_ids from boolean expressions"
    },
    {
      "name": "tvidlist_ids",
      "type": {
          "type": "array",
          "items": "long"
      },
      "doc": "tvlist_id from boolean expression"
    },
    {
      "name": "ias_segments",
      "type": ["null", {
          "type": "array",
          "items": "long"
      }],
      "doc": "ias_segments from boolean expressions"
    },
    {
      "name": "exchange_seller_app_id",
      "type": ["null","long"],
      "doc": "???"
    },
    {
      "name": "exchange_seller_site_id",
      "type": ["null", "long"],
      "doc": "???"
    },
    {
      "name": "geo_radius_list_ids",
      "type": ["null", {
          "type": "array",
          "items": "long"
      }],
      "doc": "rtb_gateway_geo_radius_index:lookup(lat,lon)"
    },
    {
      "name": "lotame_segments",
      "type": ["null", {
          "type": "array",
          "items": "long"
      }],
      "doc": "lotame_segments from boolean expressions"
    },
    {
      "name": "liveramp_1p_segments",
      "type": ["null", {
          "type": "array",
          "items": "long"
      }],
      "doc": "liveramp_1p_segments from boolean expressions"
    },
    {
      "name": "liveramp_3p_segments",
      "type": ["null", {
          "type": "array",
          "items": "long"
      }],
      "doc": "liveramp_3p_segments from boolean expressions"
    },
    {
      "name": "adobe_aam_segments",
      "type": ["null", {
          "type": "array",
          "items": "long"
      }],
      "doc": "adobe_aam_segments from boolean expressions"
    },
    {
      "name": "nielsen_segments",
      "type": ["null", {
          "type": "array",
          "items": "long"
      }],
      "doc": "nielsen_segments from boolean expressions"
    },
    {
      "name": "bluekai_segments",
      "type": ["null", {
          "type": "array",
          "items": "long"
      }],
      "doc": "bluekai_segments from boolean expressions"
    },
    {
      "name": "bluekai_3p_segments",
      "type": ["null", {
          "type": "array",
          "items": "long"
      }],
      "doc": "bluekai_3p_segments from boolean expressions"
    },
    {
      "name": "bluekai_custom_segments",
      "type": ["null", {
          "type": "array",
          "items": "long"
      }],
      "doc": "bluekai_custom_segments from boolean expressions"
    },
    {
      "name": "neustar_segments",
      "type": ["null", {
          "type": "array",
          "items": "long"
      }],
      "doc": "neustar_segments from boolean expressions"
    },
    {
      "name": "krux_1p_segments",
      "type": ["null", {
          "type": "array",
          "items": "long"
      }],
      "doc": "krux_1p_segments from boolean expressions"
    },
    {
      "name": "unified_segments",
      "type": ["null", {
          "type": "array",
          "items": "long"
      }],
      "doc": "unified_segments from boolean expressions"
    },
    {
      "name": "applist_ids",
      "type": ["null", {
          "type": "array",
          "items": "long"
      }],
      "doc": "app list from boolean expressions"
    },
    {
      "name": "impression_type",
      "type": ["null","string"],
      "doc": "Impression type: site,app, ..."
    },
    {
      "name": "publisher_id",
      "type": ["null", "long"],
      "doc": "publisher_id from boolean expressions"
    },
    {"name": "ssl","type": ["null","boolean"], "doc": "Using SSL"},
    {
      "name": "isp",
      "type": ["null","string"],
      "doc": "Internet Service Provider"
    },
    {
      "name": "org_name",
      "type": ["null","string"],
      "doc": "Internet Service Provider"
    },
    {
      "name": "uid_type",
      "type": ["null", "long"],
      "doc": "Type of UID: uuid, appleifa, samsungtvid, rokurida ...."
    },
    {
      "name": "genre",
      "type": ["null","string"],
      "doc": "Genre that best describes the content (e.g., rock, pop, etc)"
    },
    {
      "name": "netspeed",
      "type": ["null","string"],
      "doc": "Network speed: cellular, dialup, cable, corporate ..."
    },
    {"name": "rand","type": "long", "doc": "granderl:uniform(1000000), using in boolena expression flight matching"},
    {
      "name": "stv_model",
      "type": ["null","string"],
      "doc": "using for fligh matching"
    },
    {
      "name": "merkle_segments",
      "type": ["null", {
          "type": "array",
          "items": "long"
      }],
      "doc": "Merkle segments list"
    },
    {
      "name": "ads_txt_relationship",
      "type": ["null", "long"],
      "doc": "direct:0, reseller:1, unknown:2, not_found:3"
    },
    {
      "name": "samsung_device_country",
      "type": ["null", "string"],
      "doc": "samsung_device_country"
    },
    {
      "name": "marketing_name",
      "type": ["null","string"],
      "doc": "Marketing name for flight matching"
    },
    {
      "name": "width",
      "type": ["null", "long"],
      "doc": "Impression width"
    },
    {
      "name": "height",
      "type": ["null", "long"],
      "doc": "Impression height"
    },
    {
      "name": "types",
      "type": {
          "type": "array",
          "items": "long"
      },
      "doc": "Allowed types of impression"
    },
    {
      "name": "position",
      "type": ["null", "long", "string"],
      "doc": "OpenRTB: Ad position on screen. Refer to List 5.4"
    },
    {
      "name": "deal_ids",
      "type": ["null",{
          "type": "array",
          "items": ["long", "string"]
      }],
      "doc": "deal_ids for flight matching"
    },
    {
      "name": "exchange_seat_ids",
      "type": ["null",{
          "type": "array",
          "items": "long"
      }],
      "doc": "exchange_seat_ids for flight matching"
    },
    {
      "name": "private",
      "type": ["null","boolean"],
      "doc": "?????"
    },
    {
      "name": "video_start_delay",
      "type": ["null", "long"],
      "doc": "OpenRTB start delay. Ref. 5.12"
    },
        {
      "name": "min_duration",
      "type": ["null", "long"],
      "doc": "Minimum ad duration in seconds"
    },
    {
      "name": "max_duration",
      "type": ["null", "long"],
      "doc": "Maximum ad duration in seconds."
    },
    {
      "name": "player_w",
      "type": ["null", "long"],
      "doc": "Width of the video player in device independent pixels (DIPS)"
    },
    {
      "name": "player_h",
      "type": ["null", "long"],
      "doc": "Height of the video player in device independent pixels (DIPS)."
    },
    {
      "name": "player_premium",
      "type": ["null","boolean"],
      "doc": "????"
    },
    {"name": "hdmi_vendor","type": ["null", "string"],"default": null, "doc": "????"},
    {
      "name": "hdmi_product",
      "type": ["null", "string"],
      "doc": "????"
    }
  ]
}
