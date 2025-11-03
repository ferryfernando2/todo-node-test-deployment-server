// Simple script to send an FCM message to a specific device token using firebase-admin
// Usage: node tools/send_fcm.js <serviceAccountJson> <targetToken> "<title>" "<body>" [dataJson]
// Example:
// node tools/send_fcm.js ./serviceAccountKey.json <TOKEN> "Hello" "You have a message" '{"chatId":"123"}'

const admin = require('firebase-admin');
const fs = require('fs');

async function main() {
  const args = process.argv.slice(2);
  if (args.length < 4) {
    console.error('Usage: node tools/send_fcm.js <serviceAccountJson> <targetToken> "<title>" "<body>" [dataJson]');
    process.exit(2);
  }

  const [serviceAccountPath, targetToken, title, body, dataJson] = args;

  if (!fs.existsSync(serviceAccountPath)) {
    console.error('serviceAccount file not found:', serviceAccountPath);
    process.exit(2);
  }

  const serviceAccount = require(serviceAccountPath);
  admin.initializeApp({ credential: admin.credential.cert(serviceAccount) });

  const message = {
    token: targetToken,
    notification: {
      title: title,
      body: body,
    },
    android: {
      priority: 'high',
      notification: {
        channelId: 'messages',
      },
    },
    apns: {
      payload: {
        aps: {
          'content-available': 1,
        },
      },
    },
    data: dataJson ? JSON.parse(dataJson) : {},
  };

  try {
    const resp = await admin.messaging().send(message);
    console.log('Message sent:', resp);
  } catch (err) {
    console.error('Error sending message:', err);
  }
}

main();
