#!/bin/bash

echo "✨ Running init-mongo.sh"

mongosh -- "$MONGO_INITDB_DATABASE" <<EOF
    var rootUser = '$MONGO_INITDB_ROOT_USERNAME';
    var rootPassword = '$MONGO_INITDB_ROOT_PASSWORD';
    var admin = db.getSiblingDB('admin');
    admin.auth(rootUser, rootPassword);

    var user = '$MONGO_USER';
    var passwd = '$MONGO_PASSWORD';
    db.createUser({user: user, pwd: passwd, roles: ["readWrite"]});

    db.createCollection('User');
    db.createCollection('Activity');
    db.createCollection('TrackPoint');
EOF

echo "✅ init-mongo.sh finished"