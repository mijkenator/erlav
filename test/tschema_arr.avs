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
      "doc": "Country 2 letter code",
      "default": null
    },
    {
      "name": "region",
      "type": ["null", "string"],
      "doc": "Region or state",
      "default": null
    },
    {
      "name": "city",
      "type": ["null", "string"],
      "doc": "City name",
      "default": null
    },
    {
      "name": "pc",
      "type": ["null", "string"],
      "default": null,
      "doc": "Postal/zip code"
    },
    {"name": "dma","type": ["null", "long"], 
      "default": null,
      "doc": "DMA code (Designated Market Area)"},
    {"name": "lg","type": ["null", "string"], 
      "default": null,
     "doc": "Language code ISO 639-1"},
    {
      "name": "doubleclick_verticals",
      "type": ["null", {"type":"array", "items":"long"}],
      "default": null,
      "doc": "??????"
    },
    {
      "name": "iab_categories",
      "type": ["null", {
          "type": "array",
          "items": "long"
      }],
      "default": null,
      "doc": "list of IAB categories for request"
    },
    {
      "name": "device_type_id",
      "type": ["null", "long"],
      "default": null,
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
      "default": null,
      "doc": "System ID from erl_adgear_ua:classify(UserAgent)"
    },
    {
      "name": "operating_system_variant_id",
      "type": ["null", "long"],
      "default": null,
      "doc": "System Varian ID from erl_adgear_ua:classify(UserAgent)"
    },
    {
      "name": "browser_id",
      "type": ["null", "long"],
      "default": null,
      "doc": "Browser ID from erl_adgear_ua:classify(UserAgent)"
    },
    {"name": "browser_variant_id","type": "long", "doc": "from erl_adgear_ua:classify(UserAgent)"},
    {
      "name": "matched_user",
      "type": ["null","boolean"],
      "default": null,
      "doc": "BuyerId /= undefined andalso not UidNew) orelse Ifa /= undefined"
    },
    {
      "name": "matched_tv",
      "type": ["null","boolean"],
      "default": null,
      "doc": "Is matched TV by boolean expression"
    },
    {
      "name": "matched_ip",
      "type": ["null","boolean"],
      "default": null,
      "doc": "Is ip matched by boolean expression"
    },
    {
      "name": "matched_idl_pid",
      "type": ["null","boolean"],
      "default": null,
      "doc": "Is idl_pid matched by boolean expression"
    },
    {
      "name": "matched_idl_hhid",
      "type": ["null","boolean"],
      "default": null,
      "doc": "Is idl_hhid matched by boolean expression"
    },
    {
      "name": "latitude",
      "type": ["null", "float"],
      "default": null,
      "doc": "Latitude from -90.0 to +90.0, where negative is south"
    },
    {
      "name": "longitude",
      "type": ["null", "float"],
      "default": null,
      "doc": "Longitude from -180.0 to +180.0, where negative is west"
    },
    {
      "name": "device_located",
      "type": ["null","boolean"],
      "default": null,
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
      "default": null,
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
      "default": null,
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
      "default": null,
      "doc": "????"
    },
    {"name": "ip","type": ["null","string"], 
     "default": null,
     "doc": "IP address from bid request"},
    {"name": "now","type": "long", "doc": "trunc(rtb_lib_utils:timestamp_to_epoch(Timestamp))"},
    {
      "name": "iplist_ids",
      "type": ["null", {
          "type": "array",
          "items": "long"
      }],
      "default": null,
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
      "default": null,
      "doc": "ias_segments from boolean expressions"
    },
    {
      "name": "exchange_seller_app_id",
      "type": ["null","long"],
      "default": null,
      "doc": "???"
    },
    {
      "name": "exchange_seller_site_id",
      "type": ["null", "long"],
      "default": null,
      "doc": "???"
    },
    {
      "name": "geo_radius_list_ids",
      "type": ["null", {
          "type": "array",
          "items": "long"
      }],
      "default": null,
      "doc": "rtb_gateway_geo_radius_index:lookup(lat,lon)"
    },
    {
      "name": "lotame_segments",
      "type": ["null", {
          "type": "array",
          "items": "long"
      }],
      "default": null,
      "doc": "lotame_segments from boolean expressions"
    },
    {
      "name": "liveramp_1p_segments",
      "type": ["null", {
          "type": "array",
          "items": "long"
      }],
      "default": null,
      "doc": "liveramp_1p_segments from boolean expressions"
    },
    {
      "name": "liveramp_3p_segments",
      "type": ["null", {
          "type": "array",
          "items": "long"
      }],
      "default": null,
      "doc": "liveramp_3p_segments from boolean expressions"
    },
    {
      "name": "adobe_aam_segments",
      "type": ["null", {
          "type": "array",
          "items": "long"
      }],
      "default": null,
      "doc": "adobe_aam_segments from boolean expressions"
    },
    {
      "name": "nielsen_segments",
      "type": ["null", {
          "type": "array",
          "items": "long"
      }],
      "default": null,
      "doc": "nielsen_segments from boolean expressions"
    },
    {
      "name": "bluekai_segments",
      "type": ["null", {
          "type": "array",
          "items": "long"
      }],
      "default": null,
      "doc": "bluekai_segments from boolean expressions"
    },
    {
      "name": "bluekai_3p_segments",
      "type": ["null", {
          "type": "array",
          "items": "long"
      }],
      "default": null,
      "doc": "bluekai_3p_segments from boolean expressions"
    },
    {
      "name": "bluekai_custom_segments",
      "type": ["null", {
          "type": "array",
          "items": "long"
      }],
      "default": null,
      "doc": "bluekai_custom_segments from boolean expressions"
    },
    {
      "name": "neustar_segments",
      "type": ["null", {
          "type": "array",
          "items": "long"
      }],
      "default": null,
      "doc": "neustar_segments from boolean expressions"
    },
    {
      "name": "krux_1p_segments",
      "type": ["null", {
          "type": "array",
          "items": "long"
      }],
      "default": null,
      "doc": "krux_1p_segments from boolean expressions"
    },
    {
      "name": "unified_segments",
      "type": ["null", {
          "type": "array",
          "items": "long"
      }],
      "default": null,
      "doc": "unified_segments from boolean expressions"
    },
    {
      "name": "applist_ids",
      "type": ["null", {
          "type": "array",
          "items": "long"
      }],
      "default": null,
      "doc": "app list from boolean expressions"
    },
    {
      "name": "impression_type",
      "type": ["null","string"],
      "default": null,
      "doc": "Impression type: site,app, ..."
    },
    {
      "name": "publisher_id",
      "type": ["null", "long"],
      "default": null,
      "doc": "publisher_id from boolean expressions"
    },
    {"name": "ssl","type": ["null","boolean"], 
    "default": null,
    "doc": "Using SSL"},
    {
      "name": "isp",
      "type": ["null","string"],
      "default": null,
      "doc": "Internet Service Provider"
    },
    {
      "name": "org_name",
      "type": ["null","string"],
      "default": null,
      "doc": "Internet Service Provider"
    },
    {
      "name": "uid_type",
      "type": ["null", "long"],
      "default": null,
      "doc": "Type of UID: uuid, appleifa, samsungtvid, rokurida ...."
    },
    {
      "name": "genre",
      "type": ["null","string"],
      "default": null,
      "doc": "Genre that best describes the content (e.g., rock, pop, etc)"
    },
    {
      "name": "netspeed",
      "type": ["null","string"],
      "default": null,
      "doc": "Network speed: cellular, dialup, cable, corporate ..."
    },
    {"name": "rand","type": "long", "doc": "granderl:uniform(1000000), using in boolena expression flight matching"},
    {
      "name": "stv_model",
      "type": ["null","string"],
      "default": null,
      "doc": "using for fligh matching"
    },
    {
      "name": "merkle_segments",
      "type": ["null", {
          "type": "array",
          "items": "long"
      }],
      "default": null,
      "doc": "Merkle segments list"
    },
    {
      "name": "ads_txt_relationship",
      "type": ["null", "long"],
      "default": null,
      "doc": "direct:0, reseller:1, unknown:2, not_found:3"
    },
    {
      "name": "samsung_device_country",
      "type": ["null", "string"],
      "default": null,
      "doc": "samsung_device_country"
    },
    {
      "name": "marketing_name",
      "type": ["null","string"],
      "default": null,
      "doc": "Marketing name for flight matching"
    },
    {
      "name": "width",
      "type": ["null", "long"],
      "default": null,
      "doc": "Impression width"
    },
    {
      "name": "height",
      "type": ["null", "long"],
      "default": null,
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
      "default": null,
      "doc": "OpenRTB: Ad position on screen. Refer to List 5.4"
    },
    {
      "name": "deal_ids",
      "type": ["null",{
          "type": "array",
          "items": "string"
      }],
      "default": null,
      "doc": "deal_ids for flight matching"
    },
    {
      "name": "exchange_seat_ids",
      "type": ["null",{
          "type": "array",
          "items": "long"
      }],
      "default": null,
      "doc": "exchange_seat_ids for flight matching"
    },
    {
      "name": "private",
      "type": ["null","boolean"],
      "default": null,
      "doc": "?????"
    },
    {
      "name": "video_start_delay",
      "type": ["null", "long"],
      "default": null,
      "doc": "OpenRTB start delay. Ref. 5.12"
    },
        {
      "name": "min_duration",
      "type": ["null", "long"],
      "default": null,
      "doc": "Minimum ad duration in seconds"
    },
    {
      "name": "max_duration",
      "type": ["null", "long"],
      "default": null,
      "doc": "Maximum ad duration in seconds."
    },
    {
      "name": "player_w",
      "type": ["null", "long"],
      "default": null,
      "doc": "Width of the video player in device independent pixels (DIPS)"
    },
    {
      "name": "player_h",
      "type": ["null", "long"],
      "default": null,
      "doc": "Height of the video player in device independent pixels (DIPS)."
    },
    {
      "name": "player_premium",
      "type": ["null","boolean"],
      "default": null,
      "doc": "????"
    },
    {"name": "hdmi_vendor","type": ["null", "string"],"default": null, "doc": "????"},
    {
      "name": "hdmi_product",
      "type": ["null", "string"],
      "default": null,
      "doc": "????"
    }
  ]
}
