#!/usr/bin/env node
/**
 * gen-vapid-keys.mjs
 *
 * VAPID 鍵ペア (P-256) を生成し、環境変数 / K8s Secret 用に出力します。
 *
 * Usage:
 *   node gen-vapid-keys.mjs
 *   node gen-vapid-keys.mjs --k8s   # K8s Secret 形式で出力
 */

import { createECDH } from "node:crypto";

const ecdh = createECDH("prime256v1");
ecdh.generateKeys();

const publicKey = ecdh.getPublicKey();   // Buffer: 65 bytes (04 || x || y)
const privateKey = ecdh.getPrivateKey(); // Buffer: 32 bytes

const vapidPublicKey = publicKey.toString("base64url");
const vapidPrivateKey = privateKey.toString("base64url");

const k8sMode = process.argv.includes("--k8s");

if (k8sMode) {
  console.log(`---
apiVersion: v1
kind: Secret
metadata:
  name: vapid-keys
  namespace: hollo-1
type: Opaque
stringData:
  VAPID_PUBLIC_KEY: "${vapidPublicKey}"
  VAPID_PRIVATE_KEY: "${vapidPrivateKey}"`);
} else {
  console.log(`# VAPID keys for hollo-stream-proxy
export VAPID_PUBLIC_KEY=${vapidPublicKey}
export VAPID_PRIVATE_KEY=${vapidPrivateKey}`);
}
