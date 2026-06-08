// Package settings loads Parley's optional non-secret TOML settings, including
// queue defaults and metadata-only agent registry overrides.
//
// Secret-safety rule: a settings file is rejected when any key whose name
// contains token, secret, password, auth, or key holds a material value. Keep
// credentials in the runner credential volume or a future broker, not in
// .parley/config.toml or global settings files.
package settings
