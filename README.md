## Build and run

```bash
cp config.sample.py config.py
# modify config.py use the right mqtt auth

docker build -t lupino/esp32-dtu-bridge:1.0.0

docker run -i -t lupino/esp32-dtu-bridge:1.0.0
```

##  modbus request
```json
{
  "method": "modbus_req",
  "modbus": "0106000f0001",
  "crc": true
}
```

```json
{
  "method": "modbus_req",
  "addr": "01",
  "op": "06",
  "reg": "000F",
  "data": "0001",
  "crc": true
}
```

```json
{
  "method": "modbus_req",
  "addr": "01",
  "op": "06",
  "reg": "000F",
  "data": 1,
  "pack_func": "uint16_AB",
  "crc": true
}
```

```json
{
  "method": "modbus_req",
  "addr": "01",
  "op": "06",
  "reg": "000F",
  "data": 1,
  "pack_func": "uint16_AB",
  "crc": true
  "parsers": [
    {
      "name": "relay_state",
      "unpack_func": "uint16_AB",
      "scale": 1,
      "offset": 0
    }
  ]
}
```

```json
{
  "method": "modbus_req",
  "addr": "01",
  "op": "06",
  "reg": "000F",
  "data": 1,
  "pack_func": "uint16_AB",
  "crc": true
  "parsers": [
    {
      "name": "relay_state",
      "unpack_func": "bin8",
      "bin_parsers": [
        {"index": 0, "name": 'name1'},
        {"index": 2, "name": 'name1'}
      ],
      "scale": 1,
      "offset": 0
    }
  ]
}
```

```json
{
  "method": "modbus_req",
  "addr": "01",
  "op": "06",
  "reg": "000F",
  "data": 1,
  "pack_func": "uint16_AB",
  "crc": true
  "parsers": [
    {
      "name": "temperature",
      "unpack_func": "uint16_AB",
      "scale": 1,
      "offset": 0
        "filters": [
          { "type": "range", "max": 80, "min": 0.5 }
          { "type": "range", "max": -0.5, "min": -40 }
          { "type": "range_ignore", "max": 0.5, "min": -0.5 }
        ]
    }
  ]
}
```
