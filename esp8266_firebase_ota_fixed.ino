#include <Arduino.h>
#include <ESP8266WiFi.h>
#include <ESP8266WebServer.h>
#include <ESP8266HTTPClient.h>
#include <ESP8266httpUpdate.h>
#include <ESP8266HTTPUpdateServer.h>
#include <WiFiClientSecureBearSSL.h>
#include <LittleFS.h>
#include <ArduinoJson.h>
#include <DHT.h>
#include <time.h>

/*
  ESP8266 + Firebase RTDB + OTA + AP config server
  -------------------------------------------------
  Pins (change if needed)
    DHT22  -> D4
    LDR    -> A0
    Relay1 -> D1  (Light)
    Relay2 -> D2  (Fan)

  Relay logic
    relay1(light): ON when temp < ifTemp1 in Temperature_based mode
    relay2(fan)  : ON when temp > ifTemp2 in Temperature_based mode

  Common 2-channel relay boards for ESP8266 are ACTIVE-LOW.
  If your relay board is ACTIVE-HIGH, change RELAY_ON / RELAY_OFF.
*/

// ============================= USER CONSTANTS =============================
#define FW_VERSION "1.0.1"
#define DHTPIN D4
#define DHTTYPE DHT22
#define RELAY1_PIN D1
#define RELAY2_PIN D2
#define LDR_PIN A0
#define RELAY_ON LOW
#define RELAY_OFF HIGH

static const char* FIREBASE_API_KEY = "AIzaSyDZWXMSdFH-EivJySs__ZtZGUHf24R0dRQ";
static const char* FIREBASE_DB_URL  = "https://pathfinder-f171e-default-rtdb.asia-southeast1.firebasedatabase.app";
static const char* OTA_BASE_URL     = "https://epi2r.org/ota/firmware-"; // + version + .bin

static const uint32_t SEND_INTERVAL_MS         = 30UL * 60UL * 1000UL; // 30 min
static const uint32_t DESIRED_POLL_MS          = 60UL * 1000UL;         // 1 min
static const uint32_t WIFI_RETRY_MS            = 30UL * 1000UL;
static const uint32_t WIFI_CONNECT_TIMEOUT_MS  = 18UL * 1000UL;
static const uint32_t OFFLINE_RESTART_MS       = 13UL * 60UL * 1000UL;  // 13 min
static const int      LIGHT_OK_LDR_MIN         = 500;                    // adjust if needed

static const char* CONFIG_FILE  = "/config.json";
static const char* AUTH_FILE    = "/auth.json";
static const char* PENDING_FILE = "/pending_data.csv";

// ============================= GLOBALS =============================
ESP8266WebServer web(80);
ESP8266HTTPUpdateServer httpUpdater;
DHT dht(DHTPIN, DHTTYPE);

struct AppConfig {
  String deviceName;
  String apSsid;
  String apPassword;
  String apIp;
  String apGateway;
  String apSubnet;
  String uplinkSsid;
  String uplinkPassword;
  unsigned long lastEpoch = 0; // persisted approximate epoch backup
};

struct FirebaseAuthState {
  String uid;
  String idToken;
  String refreshToken;
  unsigned long expiresAt = 0; // millis()
};

struct DesiredState {
  String targetVersion = FW_VERSION;
  String relay1Mode = "Temperature_based";
  String relay2Mode = "Temperature_based";
  bool relay1Manual = false;
  bool relay2Manual = false;
  float ifTemp1 = 26.0;
  float ifTemp2 = 30.0;
  unsigned long updatedAt = 0;
};

struct Sample {
  unsigned long ts = 0;
  bool sensorOk = false;
  float temp = NAN;
  float hum = NAN;
  int ldr = 0;
  bool relay1 = false;
  bool relay2 = false;
  bool lightFault = false;
  String err;
};

AppConfig cfg;
FirebaseAuthState fb;
DesiredState desired;

String deviceId;
unsigned long lastSendMs = 0;
unsigned long lastDesiredPollMs = 0;
unsigned long lastWifiTryMs = 0;
unsigned long wifiAttemptStartedMs = 0;
unsigned long offlineSinceMs = 0;
unsigned long lastTimeSyncMs = 0;
unsigned long lastKnownEpoch = 0;
bool wifiConnecting = false;
bool apStarted = false;
bool wasWifiConnected = false;
unsigned long lastConnectActionMs = 0;

// ============================= HELPERS =============================
String htmlHeader(const String& title) {
  return "<!doctype html><html><head><meta charset='utf-8'><meta name='viewport' content='width=device-width,initial-scale=1'>"
         "<title>" + title + "</title>"
         "<style>body{font-family:Arial,sans-serif;max-width:920px;margin:20px auto;padding:0 12px}"
         "nav a{margin-right:12px}table{border-collapse:collapse;width:100%}td,th{border:1px solid #ccc;padding:6px}"
         "input,select{width:100%;padding:7px;margin:4px 0 10px}button{padding:8px 12px}"
         ".card{border:1px solid #ddd;border-radius:10px;padding:12px;margin:10px 0}.ok{color:green}.bad{color:#b00020}</style>"
         "</head><body><nav><a href='/'>Home</a><a href='/config'>Config</a><a href='/pending'>Pending</a><a href='/update'>OTA Upload</a></nav><hr>";
}
String htmlFooter() { return "</body></html>"; }

String chipHex() {
  char buf[16];
  snprintf(buf, sizeof(buf), "%06X", ESP.getChipId());
  return String(buf);
}

String jsonEscape(const String& s) {
  String out;
  out.reserve(s.length() + 8);
  for (size_t i = 0; i < s.length(); i++) {
    char c = s[i];
    if (c == '"' || c == '\\') { out += '\\'; out += c; }
    else if (c == '\n') out += "\\n";
    else if (c == '\r') out += "\\r";
    else out += c;
  }
  return out;
}

bool ensureDefaultConfig() {
  if (LittleFS.exists(CONFIG_FILE)) return true;

  DynamicJsonDocument doc(768);
  doc["device_name"] = "ESP-" + chipHex();
  JsonObject ap = doc.createNestedObject("ap");
  ap["ssid"] = "ESP_AP";
  ap["password"] = "12345678";
  ap["ip"] = "192.168.5.1";
  ap["gateway"] = "192.168.5.1";
  ap["subnet"] = "255.255.255.0";
  JsonObject uplink = doc.createNestedObject("uplink");
  uplink["ssid"] = "";
  uplink["password"] = "";
  doc["last_epoch"] = 0;

  File f = LittleFS.open(CONFIG_FILE, "w");
  if (!f) return false;
  serializeJsonPretty(doc, f);
  f.close();
  return true;
}

bool ensurePendingFile() {
  if (LittleFS.exists(PENDING_FILE)) return true;
  File f = LittleFS.open(PENDING_FILE, "w");
  if (!f) return false;
  f.println("ts,sensorOk,temp,hum,ldr,relay1,relay2,lightFault,error");
  f.close();
  return true;
}

bool loadConfig() {
  if (!ensureDefaultConfig()) return false;
  File f = LittleFS.open(CONFIG_FILE, "r");
  if (!f) return false;
  DynamicJsonDocument doc(1024);
  DeserializationError err = deserializeJson(doc, f);
  f.close();
  if (err) return false;

  cfg.deviceName     = doc["device_name"] | ("ESP-" + chipHex());
  cfg.apSsid         = doc["ap"]["ssid"] | "ESP_AP";
  cfg.apPassword     = doc["ap"]["password"] | "12345678";
  cfg.apIp           = doc["ap"]["ip"] | "192.168.5.1";
  cfg.apGateway      = doc["ap"]["gateway"] | "192.168.5.1";
  cfg.apSubnet       = doc["ap"]["subnet"] | "255.255.255.0";
  cfg.uplinkSsid     = doc["uplink"]["ssid"] | "";
  cfg.uplinkPassword = doc["uplink"]["password"] | "";
  cfg.lastEpoch      = doc["last_epoch"] | 0;
  lastKnownEpoch     = cfg.lastEpoch;
  return true;
}

bool saveConfig() {
  DynamicJsonDocument doc(768);
  doc["device_name"] = cfg.deviceName;
  JsonObject ap = doc.createNestedObject("ap");
  ap["ssid"] = cfg.apSsid;
  ap["password"] = cfg.apPassword;
  ap["ip"] = cfg.apIp;
  ap["gateway"] = cfg.apGateway;
  ap["subnet"] = cfg.apSubnet;
  JsonObject uplink = doc.createNestedObject("uplink");
  uplink["ssid"] = cfg.uplinkSsid;
  uplink["password"] = cfg.uplinkPassword;
  doc["last_epoch"] = cfg.lastEpoch;

  File f = LittleFS.open(CONFIG_FILE, "w");
  if (!f) return false;
  serializeJsonPretty(doc, f);
  f.close();
  return true;
}

bool loadAuth() {
  if (!LittleFS.exists(AUTH_FILE)) return false;
  File f = LittleFS.open(AUTH_FILE, "r");
  if (!f) return false;
  DynamicJsonDocument doc(512);
  auto err = deserializeJson(doc, f);
  f.close();
  if (err) return false;
  fb.uid = doc["uid"] | "";
  fb.idToken = doc["idToken"] | "";
  fb.refreshToken = doc["refreshToken"] | "";
  fb.expiresAt = 0;
  return fb.refreshToken.length() > 0;
}

bool saveAuth() {
  DynamicJsonDocument doc(512);
  doc["uid"] = fb.uid;
  doc["idToken"] = fb.idToken;
  doc["refreshToken"] = fb.refreshToken;
  File f = LittleFS.open(AUTH_FILE, "w");
  if (!f) return false;
  serializeJson(doc, f);
  f.close();
  return true;
}

String monthKey(unsigned long ts) {
  time_t t = (time_t)ts;
  struct tm *tmr = gmtime(&t);
  char buf[8];
  snprintf(buf, sizeof(buf), "%04d%02d", tmr->tm_year + 1900, tmr->tm_mon + 1);
  return String(buf);
}

unsigned long approxEpoch() {
  time_t now = time(nullptr);
  if (now > 1700000000UL) {
    lastKnownEpoch = (unsigned long)now;
    cfg.lastEpoch = lastKnownEpoch;
    saveConfig();
    lastTimeSyncMs = millis();
    return (unsigned long)now;
  }
  if (lastKnownEpoch > 0) {
    return lastKnownEpoch + (millis() - lastTimeSyncMs) / 1000UL;
  }
  return 0;
}

void syncTimeIfPossible() {
  if (WiFi.status() == WL_CONNECTED) {
    configTime(0, 0, "pool.ntp.org", "time.nist.gov", "time.google.com");
    time_t now = time(nullptr);
    unsigned long start = millis();
    while (now < 1700000000UL && millis() - start < 4000UL) {
      delay(100);
      now = time(nullptr);
    }
    if (now > 1700000000UL) {
      lastKnownEpoch = (unsigned long)now;
      lastTimeSyncMs = millis();
      cfg.lastEpoch = lastKnownEpoch;
      saveConfig();
    }
  }
}

String pendingCountText() {
  File f = LittleFS.open(PENDING_FILE, "r");
  if (!f) return "0";
  int count = -1;
  while (f.available()) { f.readStringUntil('\n'); count++; }
  f.close();
  if (count < 0) count = 0;
  return String(count);
}

void logLine(const String& s) {
  Serial.println(s);
}

// ============================= WIFI/AP =============================
void startAP() {
  IPAddress ip, gw, sn;
  ip.fromString(cfg.apIp);
  gw.fromString(cfg.apGateway);
  sn.fromString(cfg.apSubnet);

  WiFi.mode(WIFI_AP_STA);
  WiFi.softAPConfig(ip, gw, sn);
  WiFi.softAP(cfg.apSsid.c_str(), cfg.apPassword.c_str());
  apStarted = true;
}

void beginWiFiConnect() {
  if (cfg.uplinkSsid.isEmpty()) return;
  if (WiFi.status() == WL_CONNECTED || wifiConnecting) return;
  WiFi.begin(cfg.uplinkSsid.c_str(), cfg.uplinkPassword.c_str());
  wifiConnecting = true;
  wifiAttemptStartedMs = millis();
  lastWifiTryMs = millis();
  if (offlineSinceMs == 0) offlineSinceMs = millis();
}

void manageWiFi() {
  if (cfg.uplinkSsid.isEmpty()) return;

  if (WiFi.status() == WL_CONNECTED) {
    wifiConnecting = false;
    offlineSinceMs = 0;
    return;
  }

  if (!wifiConnecting && millis() - lastWifiTryMs > WIFI_RETRY_MS) {
    beginWiFiConnect();
  }

  if (wifiConnecting && millis() - wifiAttemptStartedMs > WIFI_CONNECT_TIMEOUT_MS) {
    WiFi.disconnect();
    wifiConnecting = false;
  }

  if (offlineSinceMs > 0 && millis() - offlineSinceMs > OFFLINE_RESTART_MS) {
    ESP.restart();
  }
}

// ============================= FIREBASE AUTH =============================
bool httpsPostJson(const String& url, const String& body, String& response, int& code) {
  std::unique_ptr<BearSSL::WiFiClientSecure> client(new BearSSL::WiFiClientSecure());
  client->setInsecure();
  HTTPClient http;
  if (!http.begin(*client, url)) return false;
  http.addHeader("Content-Type", "application/json");
  code = http.POST(body);
  response = http.getString();
  http.end();
  return true;
}

bool httpsPostForm(const String& url, const String& body, String& response, int& code) {
  std::unique_ptr<BearSSL::WiFiClientSecure> client(new BearSSL::WiFiClientSecure());
  client->setInsecure();
  HTTPClient http;
  if (!http.begin(*client, url)) return false;
  http.addHeader("Content-Type", "application/x-www-form-urlencoded");
  code = http.POST(body);
  response = http.getString();
  http.end();
  return true;
}

bool firebaseAnonymousSignup() {
  String url = String("https://identitytoolkit.googleapis.com/v1/accounts:signUp?key=") + FIREBASE_API_KEY;
  String response;
  int code = 0;
  if (!httpsPostJson(url, "{\"returnSecureToken\":true}", response, code)) return false;
  if (code != 200) return false;

  DynamicJsonDocument doc(768);
  if (deserializeJson(doc, response)) return false;
  fb.uid = doc["localId"] | "";
  fb.idToken = doc["idToken"] | "";
  fb.refreshToken = doc["refreshToken"] | "";
  const char* expiresInStr = doc["expiresIn"] | "3600";
  int expiresIn = atoi(expiresInStr);
  fb.expiresAt = millis() + (expiresIn - 120) * 1000UL;
  saveAuth();
  return fb.idToken.length() > 0;
}

bool firebaseRefreshToken() {
  if (fb.refreshToken.isEmpty()) return false;
  String url = String("https://securetoken.googleapis.com/v1/token?key=") + FIREBASE_API_KEY;
  String body = "grant_type=refresh_token&refresh_token=" + fb.refreshToken;
  String response;
  int code = 0;
  if (!httpsPostForm(url, body, response, code)) return false;
  if (code != 200) return false;

  DynamicJsonDocument doc(768);
  if (deserializeJson(doc, response)) return false;
  fb.uid = doc["user_id"] | fb.uid;
  fb.idToken = doc["id_token"] | "";
  fb.refreshToken = doc["refresh_token"] | fb.refreshToken;
  const char* expiresInStr = doc["expires_in"] | "3600";
  int expiresIn = atoi(expiresInStr);
  fb.expiresAt = millis() + (expiresIn - 120) * 1000UL;
  saveAuth();
  return fb.idToken.length() > 0;
}

bool ensureFirebaseAuth() {
  if (WiFi.status() != WL_CONNECTED) return false;
  if (fb.idToken.length() > 0 && millis() < fb.expiresAt) return true;
  if (fb.refreshToken.length() > 0 && firebaseRefreshToken()) return true;
  fb = FirebaseAuthState();
  return firebaseAnonymousSignup();
}

bool firebaseRequest(const String& method, const String& path, const String& body, String* response, bool silentWrite = false) {
  bool haveAuth = ensureFirebaseAuth();

  String url = String(FIREBASE_DB_URL) + path + ".json";
  bool hasQuery = false;
  if (haveAuth && fb.idToken.length() > 0) {
    url += "?auth=" + fb.idToken;
    hasQuery = true;
  }
  if (silentWrite && (method == "PUT" || method == "POST" || method == "PATCH")) {
    url += hasQuery ? "&print=silent" : "?print=silent";
  }

  std::unique_ptr<BearSSL::WiFiClientSecure> client(new BearSSL::WiFiClientSecure());
  client->setInsecure();
  HTTPClient http;
  if (!http.begin(*client, url)) return false;

  int code = 0;
  if (method == "GET") code = http.GET();
  else {
    http.addHeader("Content-Type", "application/json");
    if (method == "PUT") code = http.PUT(body);
    else if (method == "POST") code = http.POST(body);
    else if (method == "PATCH") code = http.sendRequest("PATCH", body);
    else if (method == "DELETE") code = http.sendRequest("DELETE", body);
  }

  String resp = http.getString();
  http.end();
  if (response) *response = resp;
  Serial.printf("[FB] %s %s -> %d\n", method.c_str(), path.c_str(), code);
  if (!(code >= 200 && code < 300) && resp.length()) {
    Serial.println(resp);
  }
  return code >= 200 && code < 300;
}

bool claimDeviceOwnership() {
  if (fb.uid.isEmpty()) return true; // public DB mode
  String response;
  if (!firebaseRequest("GET", "/deviceOwners/" + deviceId, "", &response, false)) return true;
  String trimmed = response;
  trimmed.trim();
  if (trimmed == "null") {
    String body = "\"" + fb.uid + "\"";
    firebaseRequest("PUT", "/deviceOwners/" + deviceId, body, nullptr, true);
    return true;
  }
  trimmed.replace("\"", "");
  if (trimmed == fb.uid) return true;
  String body = "\"" + fb.uid + "\"";
  firebaseRequest("PUT", "/deviceOwners/" + deviceId, body, nullptr, true);
  return true;
}

void publishMetaAndState(bool includeDesiredDefaults = false) {
  unsigned long nowTs = approxEpoch();
  String metaBody = "{";
  metaBody += "\"deviceId\":\"" + jsonEscape(deviceId) + "\",";
  metaBody += "\"deviceName\":\"" + jsonEscape(cfg.deviceName) + "\",";
  metaBody += "\"firmwareVersion\":\"" + String(FW_VERSION) + "\",";
  metaBody += "\"otaBaseUrl\":\"" + String(OTA_BASE_URL) + "\",";
  metaBody += "\"lastSeen\":" + String(nowTs) + ",";
  metaBody += "\"apSsid\":\"" + jsonEscape(cfg.apSsid) + "\"";
  metaBody += "}";
  firebaseRequest("PATCH", "/devices/" + deviceId + "/meta", metaBody, nullptr, true);

  String stateBody = "{";
  stateBody += "\"online\":true,";
  stateBody += "\"lastSeen\":" + String(nowTs) + ",";
  stateBody += "\"wifiConnected\":" + String(WiFi.status() == WL_CONNECTED ? "true" : "false") + ",";
  stateBody += "\"ip\":\"" + WiFi.localIP().toString() + "\",";
  stateBody += "\"rssi\":" + String(WiFi.status() == WL_CONNECTED ? WiFi.RSSI() : 0) + ",";
  stateBody += "\"pendingCount\":" + pendingCountText() + ",";
  stateBody += "\"firmwareVersion\":\"" + String(FW_VERSION) + "\"";
  stateBody += "}";
  firebaseRequest("PATCH", "/devices/" + deviceId + "/state", stateBody, nullptr, true);

  if (includeDesiredDefaults) {
    String desiredGet;
    if (firebaseRequest("GET", "/devices/" + deviceId + "/desired", "", &desiredGet, false)) {
      String t = desiredGet; t.trim();
      if (t == "null") {
        String body = "{";
        body += "\"targetVersion\":\"" + String(FW_VERSION) + "\",";
        body += "\"relay1Mode\":\"Temperature_based\",";
        body += "\"relay2Mode\":\"Temperature_based\",";
        body += "\"relay1Manual\":false,";
        body += "\"relay2Manual\":false,";
        body += "\"ifTemp1\":26,";
        body += "\"ifTemp2\":30,";
        body += "\"updatedAt\":" + String(nowTs);
        body += "}";
        firebaseRequest("PUT", "/devices/" + deviceId + "/desired", body, nullptr, true);
      }
    }
  }
}

void publishSampleState(const Sample& s) {
  String tempJson = s.sensorOk ? String(s.temp, 2) : String("null");
  String humJson  = s.sensorOk ? String(s.hum, 2)  : String("null");
  String stateBody = "{";
  stateBody += "\"online\":true,";
  stateBody += "\"wifiConnected\":" + String(WiFi.status() == WL_CONNECTED ? "true" : "false") + ",";
  stateBody += "\"ip\":\"" + (WiFi.status() == WL_CONNECTED ? WiFi.localIP().toString() : String("0.0.0.0")) + "\",";
  stateBody += "\"rssi\":" + String(WiFi.status() == WL_CONNECTED ? WiFi.RSSI() : 0) + ",";
  stateBody += "\"lastSeen\":" + String(approxEpoch()) + ",";
  stateBody += "\"lastUploadTs\":" + String(s.ts) + ",";
  stateBody += "\"pendingCount\":" + pendingCountText() + ",";
  stateBody += "\"sensorOk\":" + String(s.sensorOk ? "true" : "false") + ",";
  stateBody += "\"temp\":" + tempJson + ",";
  stateBody += "\"hum\":" + humJson + ",";
  stateBody += "\"ldr\":" + String(s.ldr) + ",";
  stateBody += "\"relay1\":" + String(s.relay1 ? "true" : "false") + ",";
  stateBody += "\"relay2\":" + String(s.relay2 ? "true" : "false") + ",";
  stateBody += "\"lightFault\":" + String(s.lightFault ? "true" : "false") + ",";
  stateBody += "\"error\":" + (s.err.length() ? ("\"" + jsonEscape(s.err) + "\"") : String("null")) + ",";
  stateBody += "\"firmwareVersion\":\"" + String(FW_VERSION) + "\"";
  stateBody += "}";
  firebaseRequest("PATCH", "/devices/" + deviceId + "/state", stateBody, nullptr, true);
}

bool fetchDesiredState() {
  String response;
  if (!firebaseRequest("GET", "/devices/" + deviceId + "/desired", "", &response, false)) return false;
  String t = response; t.trim();
  if (t == "null" || t.isEmpty()) return false;

  DynamicJsonDocument doc(768);
  if (deserializeJson(doc, response)) return false;
  desired.targetVersion = doc["targetVersion"] | desired.targetVersion;
  desired.relay1Mode    = doc["relay1Mode"] | desired.relay1Mode;
  desired.relay2Mode    = doc["relay2Mode"] | desired.relay2Mode;
  desired.relay1Manual  = doc["relay1Manual"] | desired.relay1Manual;
  desired.relay2Manual  = doc["relay2Manual"] | desired.relay2Manual;
  desired.ifTemp1       = doc["ifTemp1"] | (doc["iftemperature1"] | desired.ifTemp1);
  desired.ifTemp2       = doc["ifTemp2"] | (doc["iftemperature2"] | desired.ifTemp2);
  desired.updatedAt     = doc["updatedAt"] | desired.updatedAt;
  return true;
}

void maybeOTAFromDesired() {
  if (WiFi.status() != WL_CONNECTED) return;
  if (desired.targetVersion.isEmpty() || desired.targetVersion == FW_VERSION) return;

  String url = String(OTA_BASE_URL) + desired.targetVersion + ".bin";
  WiFiClientSecure client;
  client.setInsecure();
  ESPhttpUpdate.setFollowRedirects(HTTPC_STRICT_FOLLOW_REDIRECTS);
  ESPhttpUpdate.rebootOnUpdate(true);
  t_httpUpdate_return ret = ESPhttpUpdate.update(client, url);
  (void)ret;
}

// ============================= SENSOR / RELAY =============================
Sample readSample() {
  Sample s;
  s.ts = approxEpoch();

  float h = dht.readHumidity();
  float t = dht.readTemperature();
  int ldr = analogRead(LDR_PIN);

  s.ldr = ldr;
  if (!isnan(t) && !isnan(h)) {
    s.sensorOk = true;
    s.temp = t;
    s.hum = h;
  } else {
    s.sensorOk = false;
    s.err = "DHT_FAIL";
  }
  return s;
}

void setRelayHardware(bool r1On, bool r2On) {
  digitalWrite(RELAY1_PIN, r1On ? RELAY_ON : RELAY_OFF);
  digitalWrite(RELAY2_PIN, r2On ? RELAY_ON : RELAY_OFF);
}

bool currentRelay1() { return digitalRead(RELAY1_PIN) == RELAY_ON; }
bool currentRelay2() { return digitalRead(RELAY2_PIN) == RELAY_ON; }

void applyRelayLogic(const Sample& sensor) {
  bool r1 = currentRelay1();
  bool r2 = currentRelay2();

  if (desired.relay1Mode == "Manual") {
    r1 = desired.relay1Manual;
  } else if (sensor.sensorOk) {
    r1 = sensor.temp < desired.ifTemp1;
  }

  if (desired.relay2Mode == "Manual") {
    r2 = desired.relay2Manual;
  } else if (sensor.sensorOk) {
    r2 = sensor.temp > desired.ifTemp2;
  }

  setRelayHardware(r1, r2);
}

Sample finalizeSample(Sample s) {
  s.relay1 = currentRelay1();
  s.relay2 = currentRelay2();
  s.lightFault = s.relay1 && (s.ldr < LIGHT_OK_LDR_MIN);
  return s;
}

String telemetryJson(const Sample& s) {
  String tempJson = s.sensorOk ? String(s.temp, 2) : String("null");
  String humJson  = s.sensorOk ? String(s.hum, 2)  : String("null");
  String body = "{";
  body += "\"ts\":" + String(s.ts) + ",";
  body += "\"sensorOk\":" + String(s.sensorOk ? "true" : "false") + ",";
  body += "\"temp\":" + tempJson + ",";
  body += "\"hum\":" + humJson + ",";
  body += "\"ldr\":" + String(s.ldr) + ",";
  body += "\"relay1\":" + String(s.relay1 ? "true" : "false") + ",";
  body += "\"relay2\":" + String(s.relay2 ? "true" : "false") + ",";
  body += "\"lightFault\":" + String(s.lightFault ? "true" : "false") + ",";
  body += "\"firmwareVersion\":\"" + String(FW_VERSION) + "\",";
  body += "\"error\":" + (s.err.length() ? ("\"" + jsonEscape(s.err) + "\"") : String("null"));
  body += "}";
  return body;
}

String telemetryCsv(const Sample& s) {
  String line;
  String tempCsv = s.sensorOk ? String(s.temp, 2) : String("");
  String humCsv  = s.sensorOk ? String(s.hum, 2)  : String("");
  line.reserve(96);
  line += String(s.ts); line += ',';
  line += (s.sensorOk ? '1' : '0'); line += ',';
  line += tempCsv; line += ',';
  line += humCsv; line += ',';
  line += String(s.ldr); line += ',';
  line += (s.relay1 ? '1' : '0'); line += ',';
  line += (s.relay2 ? '1' : '0'); line += ',';
  line += (s.lightFault ? '1' : '0'); line += ',';
  line += s.err;
  return line;
}

Sample csvToSample(const String& line) {
  Sample s;
  int idx[9];
  int found = 0;
  for (unsigned int i = 0; i < line.length() && found < 9; i++) {
    if (line[i] == ',') idx[found++] = i;
  }
  if (found < 8) { s.err = "CSV_PARSE"; return s; }

  auto part = [&](int a, int b)->String {
    return line.substring(a, b);
  };

  int p0 = -1;
  String c0 = part(p0 + 1, idx[0]);
  String c1 = part(idx[0] + 1, idx[1]);
  String c2 = part(idx[1] + 1, idx[2]);
  String c3 = part(idx[2] + 1, idx[3]);
  String c4 = part(idx[3] + 1, idx[4]);
  String c5 = part(idx[4] + 1, idx[5]);
  String c6 = part(idx[5] + 1, idx[6]);
  String c7 = part(idx[6] + 1, idx[7]);
  String c8 = line.substring(idx[7] + 1);

  s.ts = strtoul(c0.c_str(), nullptr, 10);
  s.sensorOk = (c1 == "1");
  s.temp = c2.length() ? c2.toFloat() : NAN;
  s.hum = c3.length() ? c3.toFloat() : NAN;
  s.ldr = c4.toInt();
  s.relay1 = (c5 == "1");
  s.relay2 = (c6 == "1");
  s.lightFault = (c7 == "1");
  s.err = c8;
  return s;
}

void queuePending(const Sample& s) {
  File f = LittleFS.open(PENDING_FILE, "a");
  if (!f) return;
  f.println(telemetryCsv(s));
  f.close();
}

bool sendOneTelemetry(const Sample& s) {
  String path = "/telemetry/" + deviceId + "/" + monthKey(s.ts == 0 ? approxEpoch() : s.ts);
  bool ok = firebaseRequest("POST", path, telemetryJson(s), nullptr, true);
  publishSampleState(s);
  return ok;
}

void flushPending() {
  if (!LittleFS.exists(PENDING_FILE)) return;
  File in = LittleFS.open(PENDING_FILE, "r");
  if (!in) return;

  String remaining = "ts,sensorOk,temp,hum,ldr,relay1,relay2,lightFault,error\n";
  bool firstLine = true;
  while (in.available()) {
    String line = in.readStringUntil('\n');
    line.trim();
    if (!line.length()) continue;
    if (firstLine) { firstLine = false; continue; }
    Sample s = csvToSample(line);
    if (s.ts == 0) s.ts = approxEpoch();
    if (!sendOneTelemetry(s)) {
      remaining += line + "\n";
    }
    yield();
  }
  in.close();

  File out = LittleFS.open(PENDING_FILE, "w");
  if (out) {
    out.print(remaining);
    out.close();
  }
}

void sendTelemetryCycle() {
  Sample s = readSample();
  applyRelayLogic(s);
  delay(50);
  s = finalizeSample(s);

  if (WiFi.status() == WL_CONNECTED && claimDeviceOwnership()) {
    publishMetaAndState(false);
    bool sent = sendOneTelemetry(s);
    if (!sent) {
      logLine("[SEND] live telemetry failed, queued");
      queuePending(s);
      publishSampleState(s);
    } else {
      logLine("[SEND] live telemetry ok, flushing pending");
      flushPending();
      publishSampleState(s);
    }
  } else {
    logLine("[SEND] no Wi-Fi, queued pending sample");
    queuePending(s);
    publishSampleState(s);
  }
}

void handleWifiConnectedEvent() {
  if (WiFi.status() != WL_CONNECTED) return;
  if (millis() - lastConnectActionMs < 5000UL) return;
  lastConnectActionMs = millis();
  logLine("[WIFI] connected, syncing/sending now");
  syncTimeIfPossible();
  claimDeviceOwnership();
  publishMetaAndState(true);
  fetchDesiredState();
  Sample s = readSample();
  applyRelayLogic(s);
  delay(50);
  s = finalizeSample(s);
  publishSampleState(s);
  flushPending();
  bool sent = sendOneTelemetry(s);
  if (!sent) {
    queuePending(s);
    publishSampleState(s);
  }
  maybeOTAFromDesired();
  lastSendMs = millis();
}

// ============================= WEB =============================
String homePage() {
  Sample s = finalizeSample(readSample());
  String html = htmlHeader("ESP8266 Home");
  html += "<div class='card'><h2>Device</h2>";
  html += "<table>";
  html += "<tr><th>Device name</th><td>" + cfg.deviceName + "</td></tr>";
  html += "<tr><th>Device ID</th><td>" + deviceId + "</td></tr>";
  html += "<tr><th>Firmware</th><td>" + String(FW_VERSION) + "</td></tr>";
  html += "<tr><th>AP SSID</th><td>" + cfg.apSsid + "</td></tr>";
  html += "<tr><th>Uplink Wi-Fi</th><td>" + (WiFi.status() == WL_CONNECTED ? WiFi.SSID() + " / " + WiFi.localIP().toString() : String("Not connected")) + "</td></tr>";
  html += "<tr><th>Pending records</th><td>" + pendingCountText() + "</td></tr>";
  html += "</table></div>";

  html += "<div class='card'><h2>Current Readings</h2><table>";
  html += "<tr><th>Heading</th><th>Value</th></tr>";
  String tText = s.sensorOk ? String(s.temp, 2) + " °C" : String("Sensor fail");
  String hText = s.sensorOk ? String(s.hum, 2) + " %" : String("Sensor fail");
  html += "<tr><td>Temperature</td><td>" + tText + "</td></tr>";
  html += "<tr><td>Humidity</td><td>" + hText + "</td></tr>";
  html += "<tr><td>LDR</td><td>" + String(s.ldr) + "</td></tr>";
  html += "<tr><td>Relay 1 (Light)</td><td>" + String(currentRelay1() ? "ON" : "OFF") + "</td></tr>";
  html += "<tr><td>Relay 2 (Fan)</td><td>" + String(currentRelay2() ? "ON" : "OFF") + "</td></tr>";
  html += "<tr><td>Light fault</td><td>" + String(s.lightFault ? "YES" : "NO") + "</td></tr>";
  html += "</table><p>Auto refreshes every 5 seconds.</p></div>";
  html += "<script>setTimeout(()=>location.reload(),5000)</script>";
  html += htmlFooter();
  return html;
}

String configPage() {
  String html = htmlHeader("Config");
  html += "<h2>Config</h2><form method='post' action='/save-config'>";
  html += "<div class='card'><h3>Device</h3><label>Device Name</label><input name='device_name' value='" + cfg.deviceName + "'></div>";

  html += "<div class='card'><h3>AP</h3>";
  html += "<label>SSID</label><input name='ap_ssid' value='" + cfg.apSsid + "'>";
  html += "<label>Password</label><input name='ap_password' value='" + cfg.apPassword + "'>";
  html += "<label>IP</label><input name='ap_ip' value='" + cfg.apIp + "'>";
  html += "<label>Gateway</label><input name='ap_gateway' value='" + cfg.apGateway + "'>";
  html += "<label>Subnet</label><input name='ap_subnet' value='" + cfg.apSubnet + "'></div>";

  html += "<div class='card'><h3>Uplink Wi-Fi</h3>";
  html += "<label>SSID</label><input name='up_ssid' value='" + cfg.uplinkSsid + "'>";
  html += "<label>Password</label><input name='up_password' value='" + cfg.uplinkPassword + "'></div>";

  html += "<button type='submit'>Save Config</button></form>";
  html += "<form method='post' action='/factory-reset' onsubmit='return confirm(\"Reset config to default?\")' style='margin-top:12px'>";
  html += "<button type='submit'>Factory Reset</button></form>";
  html += htmlFooter();
  return html;
}

String pendingPage() {
  String html = htmlHeader("Pending Data");
  html += "<h2>Pending Data</h2><div class='card'><pre>";
  File f = LittleFS.open(PENDING_FILE, "r");
  if (f) {
    while (f.available()) html += f.readStringUntil('\n') + "\n";
    f.close();
  } else html += "pending_data.csv missing";
  html += "</pre></div>";
  html += htmlFooter();
  return html;
}

void setupWeb() {
  web.on("/", HTTP_GET, [](){ web.send(200, "text/html", homePage()); });
  web.on("/config", HTTP_GET, [](){ web.send(200, "text/html", configPage()); });
  web.on("/pending", HTTP_GET, [](){ web.send(200, "text/html", pendingPage()); });

  web.on("/save-config", HTTP_POST, [](){
    cfg.deviceName     = web.arg("device_name");
    cfg.apSsid         = web.arg("ap_ssid");
    cfg.apPassword     = web.arg("ap_password");
    cfg.apIp           = web.arg("ap_ip");
    cfg.apGateway      = web.arg("ap_gateway");
    cfg.apSubnet       = web.arg("ap_subnet");
    cfg.uplinkSsid     = web.arg("up_ssid");
    cfg.uplinkPassword = web.arg("up_password");
    saveConfig();
    web.send(200, "text/html", htmlHeader("Saved") + "<p>Config saved. Rebooting...</p>" + htmlFooter());
    delay(800);
    ESP.restart();
  });

  web.on("/factory-reset", HTTP_POST, [](){
    LittleFS.remove(CONFIG_FILE);
    LittleFS.remove(AUTH_FILE);
    ensureDefaultConfig();
    loadConfig();
    web.send(200, "text/html", htmlHeader("Reset") + "<p>Factory reset done. Rebooting...</p>" + htmlFooter());
    delay(800);
    ESP.restart();
  });

  httpUpdater.setup(&web, "/update");
  web.begin();
}

// ============================= SETUP / LOOP =============================
void setup() {
  Serial.begin(115200);
  delay(100);

  pinMode(RELAY1_PIN, OUTPUT);
  pinMode(RELAY2_PIN, OUTPUT);
  setRelayHardware(false, false);

  dht.begin();

  LittleFS.begin();
  ensureDefaultConfig();
  ensurePendingFile();
  loadConfig();
  loadAuth();

  deviceId = "esp8266_" + chipHex();
  startAP();
  setupWeb();
  beginWiFiConnect();

  Sample s = readSample();
  applyRelayLogic(s);
  delay(50);
  s = finalizeSample(s);

  // Keep one boot sample locally so it can flush as soon as Wi-Fi connects.
  queuePending(s);

  if (WiFi.status() == WL_CONNECTED) {
    handleWifiConnectedEvent();
    wasWifiConnected = true;
  }
}

void loop() {
  web.handleClient();
  manageWiFi();

  bool wifiNow = (WiFi.status() == WL_CONNECTED);
  if (wifiNow && !wasWifiConnected) {
    handleWifiConnectedEvent();
  }
  wasWifiConnected = wifiNow;

  if (wifiNow && lastKnownEpoch == 0) syncTimeIfPossible();

  if (wifiNow && millis() - lastDesiredPollMs > DESIRED_POLL_MS) {
    lastDesiredPollMs = millis();
    if (claimDeviceOwnership()) {
      fetchDesiredState();
      Sample s = readSample();
      applyRelayLogic(s);
      delay(50);
      s = finalizeSample(s);
      publishMetaAndState(false);
      publishSampleState(s);
      maybeOTAFromDesired();
    }
  }

  if (millis() - lastSendMs > SEND_INTERVAL_MS) {
    lastSendMs = millis();
    sendTelemetryCycle();
  }

  Sample s = readSample();
  applyRelayLogic(s);
  delay(300);
}
