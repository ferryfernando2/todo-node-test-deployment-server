const fs = require('fs').promises;
const path = require('path');
const { Pool } = require('pg');
let SQL = null;
const bcrypt = require('bcryptjs');
const speakeasy = require('speakeasy');

class DatabaseHandler {
    constructor() {
    // JSON fallback removed: only SQLite or Postgres supported
    this.usePostgres = !!process.env.DATABASE_URL;
    this.pgPool = null;
    // default to SQLite when DATABASE_URL is not provided unless USE_SQLITE is explicitly set
    this.useSqlite = (process.env.USE_SQLITE !== undefined) ? !!process.env.USE_SQLITE : !this.usePostgres;
    this.sqliteDb = null; // will hold sql.js Database instance
    this._sqliteFilePath = path.join(__dirname, '../database/appchat.sqlite');
    this._migrationFlagPath = path.join(__dirname, '../database/.migrated_to_postgres');
        // sqlite write queue to batch operations and reduce exports to disk
        this.sqliteWriteQueue = [];
        this._sqliteFlushTimer = null;
        this._sqliteFlushInProgress = false;
        this.sqliteStmts = {}; // prepared statements cache
    }

    // --- Group support helpers ---
    async createGroup(groupId, name, ownerId, members = []) {
        const now = new Date().toISOString();
        if (this.usePostgres) {
            const client = await this.pgPool.connect();
            try {
                await client.query('INSERT INTO groups(id,name,ownerid,metadata,createdat) VALUES($1,$2,$3,$4,$5)', [groupId, name, ownerId, JSON.stringify({}), now]);
                for (const m of members) {
                    await client.query('INSERT INTO group_members(groupid,userid,role,joinedat) VALUES($1,$2,$3,$4)', [groupId, m, 'member', now]);
                }
                return { id: groupId, name, ownerId, members, createdAt: now };
            } finally { client.release(); }
        }
        if (this.useSqlite && this.sqliteDb) {
            try {
                this.sqliteDb.run('INSERT OR REPLACE INTO groups(id,name,ownerid,metadata,createdat) VALUES(?,?,?,?,?)', [groupId, name, ownerId, JSON.stringify({}), now]);
                for (const m of members) {
                    this.sqliteDb.run('INSERT INTO group_members(groupid,userid,role,joinedat) VALUES(?,?,?,?)', [groupId, m, 'member', now]);
                }
                this.sqliteWriteQueue.push(Date.now());
                return { id: groupId, name, ownerId, members, createdAt: now };
            } catch (e) { throw e; }
        }
        throw new Error('No supported DB backend for createGroup');
    }

    async addGroupMember(groupId, userId, role = 'member') {
        const now = new Date().toISOString();
        if (this.usePostgres) {
            const client = await this.pgPool.connect();
            try {
                await client.query('INSERT INTO group_members(groupid,userid,role,joinedat) VALUES($1,$2,$3,$4)', [groupId, userId, role, now]);
                return true;
            } finally { client.release(); }
        }
        if (this.useSqlite && this.sqliteDb) {
            try {
                this.sqliteDb.run('INSERT INTO group_members(groupid,userid,role,joinedat) VALUES(?,?,?,?)', [groupId, userId, role, now]);
                this.sqliteWriteQueue.push(Date.now());
                return true;
            } catch (e) { return false; }
        }
        return false;
    }

    async getGroupMembers(groupId) {
        if (this.usePostgres) {
            const client = await this.pgPool.connect();
            try {
                const res = await client.query('SELECT userid,role,joinedat FROM group_members WHERE groupid = $1', [groupId]);
                return res.rows.map(r => ({ id: r.userid, role: r.role, joinedAt: r.joinedat }));
            } finally { client.release(); }
        }
        if (this.useSqlite && this.sqliteDb) {
            try {
                const res = this.sqliteDb.exec('SELECT userid,role,joinedat FROM group_members WHERE groupid = ?', [groupId]);
                if (!res || !res[0]) return [];
                const cols = res[0].columns; return res[0].values.map(row => { const o = {}; for (let i=0;i<cols.length;i++) o[cols[i]] = row[i]; return { id: o.userid, role: o.role, joinedAt: o.joinedat }; });
            } catch (e) { return []; }
        }
        return [];
    }

    async saveGroupMessage(groupId, fromId, message) {
        // Validate group and membership first
        const members = await this.getGroupMembers(groupId);
        if (!members || !members.length) {
            throw new Error('Group not found or empty');
        }
        
        const isMember = members.some(m => String(m.id || m.userid || m) === fromId);
        if (!isMember) {
            throw new Error('Sender is not a member of this group');
        }

    // Ensure message is stored as a string so JSON payloads (including key metadata) are preserved
    const messageToStore = (typeof message === 'string') ? message : JSON.stringify(message);

    // Use consistent chatId format: group_<groupId>
        const chatId = `group_${groupId}`;
        const timestamp = new Date().toISOString();
        const messageId = `gmsg_${Date.now()}_${Math.random().toString(36).slice(2,6)}`;

        if (this.usePostgres) {
            const client = await this.pgPool.connect();
            try {
                // Store message with explicit group metadata
                await client.query(
                    'INSERT INTO messages(id,chatid,fromid,toid,message,timestamp,encrypted,status,metadata) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)', 
                    [
                        messageId, 
                        chatId, 
                        fromId, 
                        chatId, 
                        messageToStore, 
                        timestamp, 
                        false, 
                        'sent',
                        JSON.stringify({ type: 'group_message', groupId })
                    ]
                );
                
                return { 
                    id: messageId, 
                    groupId, 
                    fromId, 
                    message, 
                    timestamp,
                    members: members.map(m => String(m.id || m.userid || m))
                };
            } finally { 
                client.release(); 
            }
        }

        if (this.useSqlite && this.sqliteDb) {
            try {
                this.sqliteDb.run(
                    'INSERT OR REPLACE INTO messages(id,chatid,fromid,toid,message,timestamp,encrypted,status,metadata) VALUES(?,?,?,?,?,?,?,?,?)',
                    [
                        messageId,
                        chatId,
                        fromId,
                        chatId,
                        messageToStore,
                        timestamp,
                        0,
                        'sent',
                        JSON.stringify({ type: 'group_message', groupId })
                    ]
                );
                
                this.sqliteWriteQueue.push(Date.now());
                
                return {
                    id: messageId,
                    groupId,
                    fromId,
                    message,
                    timestamp,
                    members: members.map(m => String(m.id || m.userid || m))
                };
            } catch (e) {
                console.error('Failed to save group message:', e);
                throw e;
            }
        }

        throw new Error('No supported DB backend for saveGroupMessage');
    }

    async getGroupMessages(groupId, options = {}) {
        const chatId = `group_${groupId}`;
        const limit = this._sanitizeLimit(options.limit);
        const before = this._sanitizeBefore(options.before);
        if (this.usePostgres) {
            const client = await this.pgPool.connect();
            try {
                const where = ['chatid = $1'];
                const args = [chatId]; let idx = 2;
                if (before) { where.push(`timestamp < $${idx++}`); args.push(before); }
                const limClause = limit ? ` LIMIT ${limit}` : '';
                const q = `SELECT * FROM messages WHERE ${where.join(' AND ')} ORDER BY timestamp ASC${limClause}`;
                const res = await client.query(q, args);
                return res.rows;
            } finally { client.release(); }
        }
        if (this.useSqlite && this.sqliteDb) {
            const whereParts = ['chatid = ?']; const args = [chatId];
            if (before) { whereParts.push('timestamp < ?'); args.push(before); }
            const lim = limit ? ` LIMIT ${limit}` : '';
            const q = `SELECT id,chatid,fromid,toid,message,timestamp,encrypted,status FROM messages WHERE ${whereParts.join(' AND ')} ORDER BY timestamp ASC${lim}`;
            const res = this.sqliteDb.exec(q, args);
            if (!res || !res[0]) return [];
            const columns = res[0].columns; const values = res[0].values;
            return values.map(row => { const obj = {}; for (let i=0;i<columns.length;i++) obj[columns[i]] = row[i]; return obj; });
        }
        return [];
    }

    async getUserGroups(userId) {
        if (this.usePostgres) {
            const client = await this.pgPool.connect();
            try {
                const res = await client.query('SELECT g.id, g.name, g.ownerid FROM groups g JOIN group_members m ON m.groupid = g.id WHERE m.userid = $1', [userId]);
                return res.rows.map(r => ({ id: r.id, name: r.name, ownerId: r.ownerid }));
            } finally { client.release(); }
        }
        if (this.useSqlite && this.sqliteDb) {
            try {
                const res = this.sqliteDb.exec('SELECT g.id, g.name, g.ownerid FROM groups g JOIN group_members m ON m.groupid = g.id WHERE m.userid = ?', [userId]);
                if (!res || !res[0]) return [];
                const cols = res[0].columns; return res[0].values.map(row => { const o = {}; for (let i=0;i<cols.length;i++) o[cols[i]] = row[i]; return { id: o.id, name: o.name, ownerId: o.ownerid }; });
            } catch (e) { return []; }
        }
        return [];
    }

    async init() {
        // If DATABASE_URL is set, initialize Postgres and create tables
        if (this.usePostgres) {
            try {
                this.pgPool = new Pool({ connectionString: process.env.DATABASE_URL });
                await this._initPostgresSchema();
                // Attempt a one-time migration from existing JSON files into Postgres
                try {
                    await this._migrateJsonToPostgres();
                } catch (merr) {
                    console.error('Migration to Postgres failed (will continue using Postgres):', merr);
                }
                console.log('Using Postgres as backend');
                return;
            } catch (e) {
                console.error('Postgres init failed:', e);
                this.usePostgres = false;
            }
        }

        // If configured to use SQLite, initialize it (simple file-based DB)
        if (this.useSqlite) {
            try {
                // sql.js exports an async initializer; call it to get the SQL object
                if (!SQL) {
                    const initSqlJs = require('sql.js');
                    SQL = await initSqlJs();
                }
                // Load existing DB file or create new
                let filebuffer = null;
                try {
                    const exists = await fs.stat(this._sqliteFilePath).then(() => true).catch(() => false);
                    if (exists) {
                        filebuffer = await fs.readFile(this._sqliteFilePath);
                    }
                } catch (e) { filebuffer = null; }

                if (filebuffer) {
                    this.sqliteDb = new SQL.Database(new Uint8Array(filebuffer));
                } else {
                    this.sqliteDb = new SQL.Database();
                }

                // Ensure tables exist
                this.sqliteDb.run(`CREATE TABLE IF NOT EXISTS users (
                    id TEXT PRIMARY KEY,
                    email TEXT,
                    password TEXT,
                    username TEXT,
                    publickey TEXT,
                    lastseen TEXT,
                    fullName TEXT,
                    bio TEXT,
                    phoneNumber TEXT,
                    location TEXT,
                    gender TEXT,
                    birthDate TEXT,
                    totpSecret TEXT,
                    profileImageUrl TEXT,
                    preferences TEXT,
                    profileVisibility TEXT DEFAULT 'public',
                    contacts TEXT
                );`);
                this.sqliteDb.run(`CREATE TABLE IF NOT EXISTS messages (id TEXT PRIMARY KEY, chatid TEXT, fromid TEXT, toid TEXT, message TEXT, timestamp TEXT, encrypted INTEGER DEFAULT 0, status TEXT, replyToId TEXT, replyToSender TEXT, replyToMessage TEXT);`);

                // Scheduled messages table
                this.sqliteDb.run(`CREATE TABLE IF NOT EXISTS scheduled_messages (
                    id TEXT PRIMARY KEY,
                    chatid TEXT,
                    fromid TEXT,
                    toid TEXT,
                    content TEXT,
                    scheduledat TEXT,
                    stampenabled INTEGER DEFAULT 1,
                    status TEXT DEFAULT 'scheduled',
                    createdat TEXT
                );`);

                // Groups and membership tables for group chats (SQLite)
                this.sqliteDb.run(`CREATE TABLE IF NOT EXISTS groups (
                    id TEXT PRIMARY KEY,
                    name TEXT,
                    ownerid TEXT,
                    metadata TEXT,
                    createdat TEXT
                );`);
                this.sqliteDb.run(`CREATE TABLE IF NOT EXISTS group_members (
                    groupid TEXT,
                    userid TEXT,
                    role TEXT DEFAULT 'member',
                    joinedat TEXT
                );`);

                // If an older DB existed, ensure columns were added (pragma table_info)
                try {
                    const info = this.sqliteDb.exec("PRAGMA table_info('users');");
                    const existing = new Set();
                    if (info && info[0] && info[0].values) {
                        for (const row of info[0].values) {
                            // PRAGMA table_info returns [cid, name, type, notnull, dflt_value, pk]
                            existing.add(row[1]);
                        }
                    }
                    const required = {
                        fullName: 'TEXT',
                        bio: 'TEXT',
                        phoneNumber: 'TEXT',
                        location: 'TEXT',
                        gender: 'TEXT',
                        birthDate: 'TEXT',
                        totpSecret: 'TEXT',
                        profileImageUrl: 'TEXT',
                        profileVisibility: "TEXT DEFAULT 'public'",
                        preferences: 'TEXT',
                        contacts: 'TEXT'
                    };
                    for (const [col, type] of Object.entries(required)) {
                        if (!existing.has(col)) {
                            try {
                                this.sqliteDb.run(`ALTER TABLE users ADD COLUMN ${col} ${type};`);
                            } catch (e) {
                                // ignore if alter fails
                            }
                        }
                    }
                } catch (e) {
                    // ignore schema upgrade failures, but log a warning
                    console.warn('Schema upgrade check failed', e && e.message ? e.message : e);
                }

                // Tune SQLite for concurrent reads and batched writes (WAL and relaxed sync for speed)
                try {
                    this.sqliteDb.run(`PRAGMA journal_mode=WAL;`);
                    this.sqliteDb.run(`PRAGMA synchronous=NORMAL;`);
                    this.sqliteDb.run(`PRAGMA temp_store=MEMORY;`);
                    this.sqliteDb.run(`PRAGMA busy_timeout=5000;`);
                } catch (e) {
                    // ignore if PRAGMA not supported
                }

                // Prepare commonly used statements for better performance
                try {
                    this.sqliteStmts.insertMessage = this.sqliteDb.prepare('INSERT OR REPLACE INTO messages (id,chatid,fromid,toid,message,timestamp,encrypted,status,replyToId,replyToSender,replyToMessage) VALUES (?,?,?,?,?,?,?,?,?,?,?)');
                    this.sqliteStmts.updateStatus = this.sqliteDb.prepare('UPDATE messages SET status = ? WHERE id = ?');
                } catch (e) {
                    // some sql.js builds may not implement prepare the same way; ignore and fallback to run
                    this.sqliteStmts = {};
                }

                // Persist initial DB file
                const data = this.sqliteDb.export();
                await fs.writeFile(this._sqliteFilePath, Buffer.from(data));

                // Start periodic flush (will no-op until queue has items)
                this._sqliteFlushTimer = setInterval(() => this._flushSqliteWrites(), 500);

                // Ensure flush on process exit
                process.on('beforeExit', () => { this._flushSqliteWritesSync(); });
                process.on('exit', () => { this._flushSqliteWritesSync(); });
                process.on('SIGINT', () => { this._flushSqliteWritesSync(); process.exit(); });

                console.log('Using sql.js (SQLite WASM) as backend');
                return;
            } catch (e) {
                console.error('sql.js init failed:', e);
                this.useSqlite = false;
             }
        }


    }

    // Helper to sanitize/limit numeric query parameters
    _sanitizeLimit(v) {
        if (v === null || v === undefined) return null;
        const n = Number(v);
        if (!Number.isFinite(n) || n <= 0) return null;
        const cap = Number(process.env.MAX_DB_LIMIT || 250);
        return Math.max(1, Math.min(Math.floor(n), cap));
    }

    _sanitizeBefore(ts) {
        if (!ts) return null;
        const d = new Date(ts);
        if (isNaN(d.getTime())) return null;
        return d.toISOString();
    }

    async _initPostgresSchema() {
        const client = await this.pgPool.connect();
        try {
                await client.query(`
                CREATE TABLE IF NOT EXISTS users (
                    id TEXT PRIMARY KEY,
                    email TEXT,
                    password TEXT,
                    username TEXT,
                    publickey TEXT,
                    lastseen TEXT,
                            fullname TEXT,
                    bio TEXT,
                    phonenumber TEXT,
                    location TEXT,
                    gender TEXT,
                    birthdate TEXT,
                            totpsecret TEXT,
                            profileimageurl TEXT,
                            preferences TEXT,
                            profilevisibility TEXT DEFAULT 'public',
                    contacts TEXT
                );
            `);

            await client.query(`
                CREATE TABLE IF NOT EXISTS messages (
                    id TEXT PRIMARY KEY,
                    chatid TEXT,
                    fromid TEXT,
                    toid TEXT,
                    message TEXT,
                    timestamp TEXT,
                    encrypted BOOLEAN DEFAULT false,
                    status TEXT,
                    replyToId TEXT,
                    replyToSender TEXT,
                    replyToMessage TEXT
                );
                CREATE TABLE IF NOT EXISTS scheduled_messages (
                    id TEXT PRIMARY KEY,
                    chatid TEXT,
                    fromid TEXT,
                    toid TEXT,
                    content TEXT,
                    scheduledat TEXT,
                    stampenabled BOOLEAN DEFAULT true,
                    status TEXT DEFAULT 'scheduled',
                    createdat TEXT
                );
                -- groups and membership for group chats
                CREATE TABLE IF NOT EXISTS groups (
                    id TEXT PRIMARY KEY,
                    name TEXT,
                    ownerid TEXT,
                    metadata JSONB,
                    createdat TEXT
                );
                CREATE TABLE IF NOT EXISTS group_members (
                    groupid TEXT,
                    userid TEXT,
                    role TEXT DEFAULT 'member',
                    joinedat TEXT
                );
            `);
        } finally {
            client.release();
        }
    }

    // JSON fallback removed; write scheduling handled by sqliteWriteQueue and periodic flush



    async createUser(email, password, username) {
        const userId = `user_${Date.now()}`;
        const now = new Date().toISOString();
        try {
            // Hash password before storing
            const hashed = password ? await bcrypt.hash(password, 10) : null;
            if (this.usePostgres) {
                const client = await this.pgPool.connect();
                try {
                    const exists = await client.query('SELECT id FROM users WHERE email=$1 LIMIT 1', [email]);
                    if (exists.rows && exists.rows.length) throw new Error('Email already exists');
                    await client.query('INSERT INTO users(id,email,password,username,publickey,lastseen,fullname,bio,phonenumber,location,gender,birthdate,totpsecret,profileimageurl,contacts) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)',
                        [userId, email, hashed, username || null, null, now, null, null, null, null, null, null, null, null, JSON.stringify([])]);
                    return this._normalizeUserObject({ id: userId, email, username, lastseen: now });
                } finally { client.release(); }
            }

            if (this.useSqlite && this.sqliteDb) {
                // ensure email unique
                const res = this.sqliteDb.exec('SELECT id FROM users WHERE email = ? LIMIT 1', [email]);
                if (res && res[0] && res[0].values && res[0].values.length) throw new Error('Email already exists');
                this.sqliteDb.run('INSERT INTO users (id,email,password,username,publickey,lastseen,fullName,bio,phoneNumber,location,gender,birthDate,totpSecret,profileImageUrl,contacts) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)', [
                    userId, email, hashed, username || null, null, now, null, null, null, null, null, null, null, null, JSON.stringify([])
                ]);
                this.sqliteWriteQueue.push(Date.now());
                return this._normalizeUserObject({ id: userId, email, username, lastseen: now });
            }

            throw new Error('No supported DB backend or DB not initialized');
        } catch (e) {
            console.error('createUser error', e);
            throw e;
        }
    }

    async loginUser(email, password) {
        if (this.usePostgres) {
            const client = await this.pgPool.connect();
            try {
                const res = await client.query('SELECT * FROM users WHERE email=$1 LIMIT 1', [email]);
                if (!res.rows || !res.rows.length) throw new Error('Invalid credentials');
                const user = res.rows[0];
                const match = user.password ? await bcrypt.compare(password, user.password) : false;
                if (!match) throw new Error('Invalid credentials');
                await client.query('UPDATE users SET lastseen = $1 WHERE id = $2', [new Date().toISOString(), user.id]);
                delete user.password;
                return this._normalizeUserObject(user);
            } finally { client.release(); }
        }
        if (this.useSqlite && this.sqliteDb) {
            const res = this.sqliteDb.exec('SELECT * FROM users WHERE email = ? LIMIT 1', [email]);
            if (!res || !res[0] || !res[0].values.length) throw new Error('Invalid credentials');
            const cols = res[0].columns;
            const vals = res[0].values[0];
            const user = {};
            for (let i = 0; i < cols.length; i++) user[cols[i]] = vals[i];
            const now = new Date().toISOString();
            this.sqliteDb.run('UPDATE users SET lastseen = ? WHERE id = ?', [now, user.id]);
            this.sqliteWriteQueue.push(Date.now());
            const match = user.password ? await bcrypt.compare(password, user.password) : false;
            if (!match) throw new Error('Invalid credentials');
            delete user.password;
            return this._normalizeUserObject(user);
        }
        throw new Error('No supported DB backend');
    }

    async updateUserStatus(userId, isOnline) {
        if (this.usePostgres) {
            const client = await this.pgPool.connect();
            try {
                await client.query('UPDATE users SET lastseen = $1 WHERE id = $2', [isOnline ? 'online' : new Date().toISOString(), userId]);
            } finally { client.release(); }
            return;
        }
        if (this.useSqlite && this.sqliteDb) {
            const lastSeen = isOnline ? 'online' : new Date().toISOString();
            this.sqliteDb.run('UPDATE users SET lastseen = ? WHERE id = ?', [lastSeen, userId]);
            this.sqliteWriteQueue.push(Date.now());
        }
    }

    async getUserContacts(userId) {
        if (this.usePostgres) {
            const client = await this.pgPool.connect();
            try {
                const r = await client.query('SELECT contacts FROM users WHERE id = $1 LIMIT 1', [userId]);
                if (!r.rows || !r.rows.length) return [];
                const contacts = r.rows[0].contacts || [];
                if (!contacts.length) return [];
                const res = await client.query(`SELECT id, username, lastseen,email,publickey,fullName,bio,phoneNumber,location,gender,birthDate,profileImageUrl,contacts FROM users WHERE id = ANY($1::text[])`, [contacts]);
                return res.rows.map(r => { if (r.password) delete r.password; return this._normalizeUserObject(r); });
            } finally { client.release(); }
        }
        if (this.useSqlite && this.sqliteDb) {
            const r = this.sqliteDb.exec('SELECT contacts FROM users WHERE id = ? LIMIT 1', [userId]);
            if (!r || !r[0] || !r[0].values.length) return [];
            let contacts = [];
            try { contacts = JSON.parse(r[0].values[0][0] || '[]'); } catch (e) { contacts = []; }
            if (!contacts.length) return [];
            const placeholders = contacts.map(() => '?').join(',');
            const res = this.sqliteDb.exec(`SELECT id, username, lastseen,email,publickey,fullName,bio,phoneNumber,location,gender,birthDate,profileImageUrl,contacts FROM users WHERE id IN (${placeholders})`, contacts);
            if (!res || !res[0]) return [];
            const cols = res[0].columns;
            return res[0].values.map(row => {
                const raw = {};
                for (let i = 0; i < cols.length; i++) raw[cols[i]] = row[i];
                if (raw.password) delete raw.password;
                return this._normalizeUserObject(raw);
            });
        }
        return [];
    }

    async getUserProfile(userId) {
        if (this.usePostgres) {
            const client = await this.pgPool.connect();
            try {
                const r = await client.query('SELECT * FROM users WHERE id = $1 LIMIT 1', [userId]);
                if (!r.rows || !r.rows.length) throw new Error('User not found');
                const user = r.rows[0]; delete user.password; return this._normalizeUserObject(user);
            } finally { client.release(); }
        }
        if (this.useSqlite && this.sqliteDb) {
            const r = this.sqliteDb.exec('SELECT * FROM users WHERE id = ? LIMIT 1', [userId]);
            if (!r || !r[0] || !r[0].values.length) throw new Error('User not found');
            const cols = r[0].columns; const vals = r[0].values[0]; const raw = {};
            for (let i = 0; i < cols.length; i++) raw[cols[i]] = vals[i];
            if (raw.password) delete raw.password;
            return this._normalizeUserObject(raw);
        }
        throw new Error('No supported DB backend');
    }

    async addContact(userId, contactId) {
        if (this.usePostgres) {
            const client = await this.pgPool.connect();
            try {
                const r = await client.query('SELECT contacts FROM users WHERE id = $1 LIMIT 1', [userId]);
                if (!r.rows || !r.rows.length) throw new Error('Your user account was not found');
                const contacts = r.rows[0].contacts || [];
                if (contacts.includes(contactId)) throw new Error('This user is already in your contacts');
                const r2 = await client.query('SELECT id, username, password FROM users WHERE id = $1 LIMIT 1', [contactId]);
                if (!r2.rows || !r2.rows.length) throw new Error('Contact not found. Please check the ID and try again');
                contacts.push(contactId);
                await client.query('UPDATE users SET contacts = $1 WHERE id = $2', [contacts, userId]);
                const contact = r2.rows[0]; if (contact.password) delete contact.password; return this._normalizeUserObject(contact);
            } finally { client.release(); }
        }
        if (this.useSqlite && this.sqliteDb) {
            const r = this.sqliteDb.exec('SELECT contacts FROM users WHERE id = ? LIMIT 1', [userId]);
            if (!r || !r[0] || !r[0].values.length) throw new Error('Your user account was not found');
            let contacts = [];
            try { contacts = JSON.parse(r[0].values[0][0] || '[]'); } catch (e) { contacts = []; }
            if (contacts.includes(contactId)) throw new Error('This user is already in your contacts');
            const r2 = this.sqliteDb.exec('SELECT id, username, password FROM users WHERE id = ? LIMIT 1', [contactId]);
            if (!r2 || !r2[0] || !r2[0].values.length) throw new Error('Contact not found. Please check the ID and try again');
            contacts.push(contactId);
            this.sqliteDb.run('UPDATE users SET contacts = ? WHERE id = ?', [JSON.stringify(contacts), userId]);
            this.sqliteWriteQueue.push(Date.now());
            const cols = r2[0].columns; const vals = r2[0].values[0]; const contact = {};
            for (let i = 0; i < cols.length; i++) contact[cols[i]] = vals[i]; if (contact.password) delete contact.password; return this._normalizeUserObject(contact);
        }
        throw new Error('No supported DB backend');
    }

    async saveMessage(fromId, toId, message, replyData) {
    if (this.usePostgres) {
            const id = `msg_${Date.now()}`;
            const chatId = [fromId, toId].sort().join('_');
            const encrypted = (() => {
                try { const p = JSON.parse(message); return !!p.encrypted; } catch(e) { return false; }
            })();
            
            // Extract reply data if provided
            const replyToId = replyData?.replyToId || null;
            const replyToSender = replyData?.replyToSender || null;
            const replyToMessage = replyData?.replyToMessage || null;
            
            const client = await this.pgPool.connect();
            try {
                await client.query(
                    'INSERT INTO messages(id, chatid, fromid, toid, message, timestamp, encrypted, status, replyToId, replyToSender, replyToMessage) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)',
                    [id, chatId, fromId, toId, message, new Date().toISOString(), encrypted, 'sent', replyToId, replyToSender, replyToMessage]
                );
            } finally { client.release(); }
            return { 
                id, 
                fromId, 
                toId, 
                message, 
                timestamp: new Date().toISOString(), 
                encrypted, 
                status: 'sent',
                replyToId,
                replyToSender,
                replyToMessage 
            };
        }
        if (this.useSqlite && this.sqliteDb) {
            const id = `msg_${Date.now()}`;
            const chatId = [fromId, toId].sort().join('_');
            let encrypted = false;
            try { const p = JSON.parse(message); encrypted = !!p.encrypted; } catch(e) { encrypted = false; }

            // Extract reply data if provided
            const replyToId = replyData?.replyToId || null;
            const replyToSender = replyData?.replyToSender || null; 
            const replyToMessage = replyData?.replyToMessage || null;

            // If prepared statement available use it and queue disk persist
            try {
                if (this.sqliteStmts.insertMessage && this.sqliteStmts.insertMessage.bind) {
                    this.sqliteStmts.insertMessage.bind([id, chatId, fromId, toId, message, new Date().toISOString(), encrypted ? 1 : 0, 'sent', replyToId, replyToSender, replyToMessage]);
                    this.sqliteStmts.insertMessage.step && this.sqliteStmts.insertMessage.step();
                    this.sqliteStmts.insertMessage.reset && this.sqliteStmts.insertMessage.reset();
                } else {
                    this.sqliteDb.run('INSERT OR REPLACE INTO messages(id,chatid,fromid,toid,message,timestamp,encrypted,status,replyToId,replyToSender,replyToMessage) VALUES(?,?,?,?,?,?,?,?,?,?,?)', 
                        [id, chatId, fromId, toId, message, new Date().toISOString(), encrypted ? 1 : 0, 'sent', replyToId, replyToSender, replyToMessage]);
                }
            } catch (e) {
                // Fallback to run
                try { 
                    this.sqliteDb.run('INSERT OR REPLACE INTO messages(id,chatid,fromid,toid,message,timestamp,encrypted,status,replyToId,replyToSender,replyToMessage) VALUES(?,?,?,?,?,?,?,?,?,?,?)', 
                        [id, chatId, fromId, toId, message, new Date().toISOString(), encrypted ? 1 : 0, 'sent', replyToId, replyToSender, replyToMessage]); 
                } catch (er) { 
                    console.error('sqlite insert failed', er); 
                }
            }

            // queue persist (fast, non-blocking)
            this.sqliteWriteQueue.push(Date.now());
            return { 
                id, 
                fromId, 
                toId, 
                message, 
                timestamp: new Date().toISOString(), 
                encrypted, 
                status: 'sent',
                replyToId,
                replyToSender,
                replyToMessage
            };
        }

    // JSON fallback removed. If not using Postgres/SQLite this code path is unsupported.
    throw new Error('No supported DB backend for saveMessage');
    }

    // Scheduled messages helpers
    async saveScheduledMessage(obj) {
        const id = obj.id || `sched_${Date.now()}`;
        const now = new Date().toISOString();
        const scheduledAt = (obj.scheduledAt) ? new Date(obj.scheduledAt).toISOString() : now;
        if (this.usePostgres) {
            const client = await this.pgPool.connect();
            try {
                await client.query('INSERT INTO scheduled_messages(id,chatid,fromid,toid,content,scheduledat,stampenabled,status,createdat) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)', [id, obj.chatId || obj.chatid || null, obj.fromId, obj.toId, obj.content || obj.message || '', scheduledAt, !!obj.stampEnabled, obj.status || 'scheduled', now]);
                return { id, scheduledAt };
            } finally { client.release(); }
        }
        if (this.useSqlite && this.sqliteDb) {
            try {
                this.sqliteDb.run('INSERT OR REPLACE INTO scheduled_messages(id,chatid,fromid,toid,content,scheduledat,stampenabled,status,createdat) VALUES(?,?,?,?,?,?,?,?,?)', [id, obj.chatId || obj.chatid || null, obj.fromId, obj.toId, obj.content || obj.message || '', scheduledAt, obj.stampEnabled ? 1 : 0, obj.status || 'scheduled', now]);
                this.sqliteWriteQueue.push(Date.now());
                return { id, scheduledAt };
            } catch (e) { throw e; }
        }
        throw new Error('No supported DB backend for scheduled messages');
    }

    async getScheduledMessagesForUser(userId) {
        if (this.usePostgres) {
            const client = await this.pgPool.connect();
            try {
                const res = await client.query('SELECT * FROM scheduled_messages WHERE fromid = $1 ORDER BY scheduledat ASC', [userId]);
                return res.rows;
            } finally { client.release(); }
        }
        if (this.useSqlite && this.sqliteDb) {
            try {
                const res = this.sqliteDb.exec('SELECT id,chatid,fromid,toid,content,scheduledat,stampenabled,status,createdat FROM scheduled_messages WHERE fromid = ? ORDER BY scheduledat ASC', [userId]);
                if (!res || !res[0]) return [];
                const cols = res[0].columns; return res[0].values.map(row => { const o = {}; for (let i=0;i<cols.length;i++) o[cols[i]] = row[i]; return o; });
            } catch (e) { return []; }
        }
        return [];
    }

    async getDueScheduledMessages(beforeIso) {
        if (this.usePostgres) {
            const client = await this.pgPool.connect();
            try {
                const res = await client.query('SELECT * FROM scheduled_messages WHERE scheduledat <= $1 AND status = $2 ORDER BY scheduledat ASC', [beforeIso, 'scheduled']);
                return res.rows;
            } finally { client.release(); }
        }
        if (this.useSqlite && this.sqliteDb) {
            try {
                const res = this.sqliteDb.exec('SELECT id,chatid,fromid,toid,content,scheduledat,stampenabled,status,createdat FROM scheduled_messages WHERE scheduledat <= ? AND status = ? ORDER BY scheduledat ASC', [beforeIso, 'scheduled']);
                if (!res || !res[0]) return [];
                const cols = res[0].columns; return res[0].values.map(row => { const o = {}; for (let i=0;i<cols.length;i++) o[cols[i]] = row[i]; return o; });
            } catch (e) { return []; }
        }
        return [];
    }

    /**
     * Attempt to claim a scheduled message so only one server instance/process will deliver it.
     * Returns true if the claim succeeded (status transitioned from 'scheduled' -> 'pending'), false otherwise.
     */
    async claimScheduledMessage(id) {
        if (!id) return false;
        if (this.usePostgres) {
            const client = await this.pgPool.connect();
            try {
                // atomically update only when status is still 'scheduled'
                const res = await client.query("UPDATE scheduled_messages SET status = $1 WHERE id = $2 AND status = $3 RETURNING id", ['pending', id, 'scheduled']);
                return (res.rows && res.rows.length > 0);
            } finally { client.release(); }
        }
        if (this.useSqlite && this.sqliteDb) {
            try {
                // sqlite doesn't support RETURNING in older builds: read-then-update with best-effort
                const cur = this.sqliteDb.exec('SELECT status FROM scheduled_messages WHERE id = ? LIMIT 1', [id]);
                if (!cur || !cur[0] || !cur[0].values.length) return false;
                const current = cur[0].values[0][0];
                if (current !== 'scheduled') return false;
                this.sqliteDb.run('UPDATE scheduled_messages SET status = ? WHERE id = ?', ['pending', id]);
                this.sqliteWriteQueue.push(Date.now());
                return true;
            } catch (e) { return false; }
        }
        return false;
    }

    async markScheduledMessageStatus(id, status) {
        if (this.usePostgres) {
            const client = await this.pgPool.connect();
            try { await client.query('UPDATE scheduled_messages SET status = $1 WHERE id = $2', [status, id]); } finally { client.release(); }
            return true;
        }
        if (this.useSqlite && this.sqliteDb) {
            try { this.sqliteDb.run('UPDATE scheduled_messages SET status = ? WHERE id = ?', [status, id]); this.sqliteWriteQueue.push(Date.now()); return true; } catch (e) { return false; }
        }
        return false;
    }

    async deleteScheduledMessage(id) {
        if (!id) return false;
        if (this.usePostgres) {
            const client = await this.pgPool.connect();
            try { await client.query('DELETE FROM scheduled_messages WHERE id = $1', [id]); } finally { client.release(); }
            return true;
        }
        if (this.useSqlite && this.sqliteDb) {
            try { this.sqliteDb.run('DELETE FROM scheduled_messages WHERE id = ?', [id]); this.sqliteWriteQueue.push(Date.now()); return true; } catch (e) { console.error('deleteScheduledMessage sqlite failed', e); return false; }
        }
        return false;
    }


    async updateMessageStatus(messageId, status) {
    if (this.usePostgres) {
            const client = await this.pgPool.connect();
            try {
                await client.query('UPDATE messages SET status = $1 WHERE id = $2', [status, messageId]);
                const res = await client.query('SELECT * FROM messages WHERE id = $1 LIMIT 1', [messageId]);
                return res.rows[0] || null;
            } finally { client.release(); }
        }

        if (this.useSqlite && this.sqliteDb) {
            try {
                if (this.sqliteStmts.updateStatus && this.sqliteStmts.updateStatus.bind) {
                    this.sqliteStmts.updateStatus.bind([status, messageId]);
                    this.sqliteStmts.updateStatus.step && this.sqliteStmts.updateStatus.step();
                    this.sqliteStmts.updateStatus.reset && this.sqliteStmts.updateStatus.reset();
                } else {
                    this.sqliteDb.run('UPDATE messages SET status = ? WHERE id = ?', [status, messageId]);
                }
            } catch (e) {
                try { this.sqliteDb.run('UPDATE messages SET status = ? WHERE id = ?', [status, messageId]); } catch (er) { console.error('sqlite update failed', er); }
            }

            // queue persist
            this.sqliteWriteQueue.push(Date.now());

            // return the updated row
            try {
                const res = this.sqliteDb.exec('SELECT id,chatid,fromid,toid,message,timestamp,encrypted,status FROM messages WHERE id = ? LIMIT 1', [messageId]);
                if (res && res[0] && res[0].values && res[0].values[0]) {
                    const cols = res[0].columns;
                    const vals = res[0].values[0];
                    const o = {};
                    for (let i = 0; i < cols.length; i++) o[cols[i]] = vals[i];
                    return o;
                }
            } catch (e) {}
            return null;
        }

    // JSON fallback removed. Unsupported when no DB backend enabled.
    throw new Error('No supported DB backend for updateMessageStatus');
    }

    /**
     * Get messages for a chat with optional pagination.
     * options: { limit: number, before: ISO8601 timestamp }
     */
    async getMessages(userId1, userId2, options = {}) {
    const chatId = [userId1, userId2].sort().join('_');
    const limit = this._sanitizeLimit(options.limit);
    const before = this._sanitizeBefore(options.before);

    if (this.usePostgres) {
        const client = await this.pgPool.connect();
        try {
            const where = ['chatid = $1'];
            const args = [chatId];
            let idx = 2;
            if (before) { where.push(`timestamp < $${idx++}`); args.push(before); }
            const order = 'ORDER BY timestamp ASC';
            // Parameterize limit using client-side cap: append LIMIT clause only if provided
            const limClause = limit ? ` LIMIT ${limit}` : '';
            const q = `SELECT * FROM messages WHERE ${where.join(' AND ')} ${order}${limClause}`;
            const res = await client.query(q, args);
            return res.rows;
        } finally { client.release(); }
    }

    if (this.useSqlite && this.sqliteDb) {
        const whereParts = ['chatid = ?'];
        const args = [chatId];
        if (before) { whereParts.push('timestamp < ?'); args.push(before); }
    const order = 'ORDER BY timestamp ASC';
    const lim = limit ? ` LIMIT ${limit}` : '';
    const q = `SELECT id,chatid,fromid,toid,message,timestamp,encrypted,status FROM messages WHERE ${whereParts.join(' AND ')} ${order}${lim}`;
    const res = this.sqliteDb.exec(q, args);
        if (!res || !res[0]) return [];
        const columns = res[0].columns;
        const values = res[0].values;
        return values.map(row => {
            const obj = {};
            for (let i = 0; i < columns.length; i++) obj[columns[i]] = row[i];
            return obj;
        });
    }

    // JSON fallback removed. If using a DB, queries above return results; otherwise return empty list.
    return [];
    }

    async saveUserPublicKey(userId, publicKey) {
        if (this.usePostgres) {
            const client = await this.pgPool.connect();
            try {
                await client.query('UPDATE users SET publickey = $1 WHERE id = $2', [publicKey, userId]);
                const r = await client.query('SELECT id,email,username,publickey,lastseen,fullName,bio,phoneNumber,location,gender,birthDate,profileImageUrl,contacts FROM users WHERE id = $1 LIMIT 1', [userId]);
                if (!r.rows || !r.rows.length) throw new Error('User not found');
                const u = r.rows[0]; if (u.password) delete u.password; return this._normalizeUserObject(u);
            } finally { client.release(); }
        }
        if (this.useSqlite && this.sqliteDb) {
            this.sqliteDb.run('UPDATE users SET publickey = ? WHERE id = ?', [publicKey, userId]);
            this.sqliteWriteQueue.push(Date.now());
            const r = this.sqliteDb.exec('SELECT id,email,username,publickey,lastseen,fullName,bio,phoneNumber,location,gender,birthDate,profileImageUrl,contacts FROM users WHERE id = ? LIMIT 1', [userId]);
            if (!r || !r[0] || !r[0].values.length) throw new Error('User not found');
            const cols = r[0].columns; const vals = r[0].values[0]; const u = {};
            for (let i = 0; i < cols.length; i++) u[cols[i]] = vals[i];
            if (u.password) delete u.password; return this._normalizeUserObject(u);
        }
        throw new Error('No supported DB backend');
    }

    // Return some lightweight stats about caches and messages to power an admin UI
    async getStats() {
        if (this.usePostgres) {
            const client = await this.pgPool.connect();
            try {
                const u = await client.query('SELECT COUNT(1) AS cnt FROM users');
                const m = await client.query('SELECT COUNT(1) AS cnt FROM messages');
                const chats = await client.query('SELECT COUNT(DISTINCT chatid) AS cnt FROM messages');
                return { usersCount: Number(u.rows[0].cnt || 0), messagesCount: Number(m.rows[0].cnt || 0), chatsCount: Number(chats.rows[0].cnt || 0) };
            } finally { client.release(); }
        }
        if (this.useSqlite && this.sqliteDb) {
            const ru = this.sqliteDb.exec('SELECT COUNT(1) AS cnt FROM users');
            const rm = this.sqliteDb.exec('SELECT COUNT(1) AS cnt FROM messages');
            const rc = this.sqliteDb.exec('SELECT COUNT(DISTINCT chatid) AS cnt FROM messages');
            const usersCount = (ru && ru[0] && ru[0].values && ru[0].values[0] && ru[0].values[0][0]) ? Number(ru[0].values[0][0]) : 0;
            const messagesCount = (rm && rm[0] && rm[0].values && rm[0].values[0] && rm[0].values[0][0]) ? Number(rm[0].values[0][0]) : 0;
            const chatsCount = (rc && rc[0] && rc[0].values && rc[0].values[0] && rc[0].values[0][0]) ? Number(rc[0].values[0][0]) : 0;
            return { usersCount, messagesCount, chatsCount };
        }
        return { usersCount: 0, messagesCount: 0, chatsCount: 0 };
    }

    /**
     * Clear all messages/chats from the database while preserving users.
     * Returns true on success, false on failure.
     */
    async clearMessages() {
        try {
            if (this.usePostgres) {
                const client = await this.pgPool.connect();
                try {
                    await client.query('DELETE FROM messages');
                    return true;
                } finally { client.release(); }
            }

            if (this.useSqlite && this.sqliteDb) {
                try {
                    this.sqliteDb.run('DELETE FROM messages');
                    // schedule a persist immediately
                    this.sqliteWriteQueue.push(Date.now());
                    await this._flushSqliteWrites();
                    return true;
                } catch (e) {
                    console.error('clearMessages sqlite failed', e);
                    return false;
                }
            }
            return false;
        } catch (e) {
            console.error('clearMessages error', e);
            return false;
        }
    }

    getPendingWriteCount() {
        return this.sqliteWriteQueue ? this.sqliteWriteQueue.length : 0;
    }

    // Force flush caches to disk immediately (used by admin endpoint)
    async flushAll() {
        try {
            if (this.useSqlite && this.sqliteDb) {
                const data = this.sqliteDb.export();
                await fs.writeFile(this._sqliteFilePath, Buffer.from(data));
                this.sqliteWriteQueue.length = 0;
                return true;
            }
            if (this.usePostgres) return true;
            return false;
        } catch (e) {
            console.error('flushAll error', e);
            return false;
        }
    }

    async searchUsers(query) {
        console.log('Searching users with query:', query);
        const isEmail = query.includes('@');
        if (this.usePostgres) {
            const client = await this.pgPool.connect();
            try {
                const q = isEmail ? 'SELECT id,username,email,lastseen FROM users WHERE email ILIKE $1 LIMIT 50' : 'SELECT id,username,email,lastseen FROM users WHERE username ILIKE $1 OR id = $2 LIMIT 50';
                const res = isEmail ? await client.query(q, [`%${query}%`]) : await client.query(q, [`%${query}%`, query]);
                return res.rows;
            } finally { client.release(); }
        }
        if (this.useSqlite && this.sqliteDb) {
            if (isEmail) {
                const r = this.sqliteDb.exec('SELECT id,username,email,lastseen FROM users WHERE LOWER(email) LIKE ? LIMIT 50', [`%${query.toLowerCase()}%`]);
                if (!r || !r[0]) return [];
                const cols = r[0].columns; return r[0].values.map(row => { const o = {}; for (let i=0;i<cols.length;i++) o[cols[i]] = row[i]; return o; });
            } else {
                const r = this.sqliteDb.exec('SELECT id,username,email,lastseen FROM users WHERE LOWER(username) LIKE ? OR id = ? LIMIT 50', [`%${query.toLowerCase()}%`, query]);
                if (!r || !r[0]) return [];
                const cols = r[0].columns; return r[0].values.map(row => { const o = {}; for (let i=0;i<cols.length;i++) o[cols[i]] = row[i]; return o; });
            }
        }
        return [];
    }

    async getMessage(messageId) {
        if (!messageId) return null;
        if (this.usePostgres) {
            const client = await this.pgPool.connect();
            try {
                const res = await client.query('SELECT * FROM messages WHERE id = $1 LIMIT 1', [messageId]);
                if (!res.rows || !res.rows.length) return null;
                return res.rows[0];
            } finally { client.release(); }
        }
        if (this.useSqlite && this.sqliteDb) {
            const res = this.sqliteDb.exec('SELECT * FROM messages WHERE id = ? LIMIT 1', [messageId]);
            if (!res || !res[0] || !res[0].values.length) return null;
            const cols = res[0].columns;
            const vals = res[0].values[0];
            const msg = {};
            for (let i = 0; i < cols.length; i++) {
                msg[cols[i]] = vals[i];
            }
            return msg;
        }
        return null;
    }

    async getUserById(userId) {
        if (this.usePostgres) {
            const client = await this.pgPool.connect();
            try {
                const r = await client.query('SELECT id,email,username,publickey,lastseen,fullName,bio,phoneNumber,location,gender,birthDate,profileImageUrl,preferences,totpsecret,profilevisibility,contacts FROM users WHERE id = $1 LIMIT 1', [userId]);
                if (!r.rows || !r.rows.length) return null;
                const u = r.rows[0]; if (u.password) delete u.password; return this._normalizeUserObject(u);
            } finally { client.release(); }
        }
        if (this.useSqlite && this.sqliteDb) {
            const r = this.sqliteDb.exec('SELECT id,email,username,publickey,lastseen,fullName,bio,phoneNumber,location,gender,birthDate,profileImageUrl,preferences,totpSecret,profileVisibility,contacts FROM users WHERE id = ? LIMIT 1', [userId]);
            if (!r || !r[0] || !r[0].values.length) return null;
            const cols = r[0].columns; const vals = r[0].values[0]; const u = {};
            for (let i = 0; i < cols.length; i++) u[cols[i]] = vals[i];
            if (u.password) delete u.password; return this._normalizeUserObject(u);
        }
        return null;
    }

    // Delete a single message by id
    async deleteMessage(messageId) {
        if (!messageId) return false;
        if (this.usePostgres) {
            const client = await this.pgPool.connect();
            try {
                await client.query('DELETE FROM messages WHERE id = $1', [messageId]);
                return true;
            } finally { client.release(); }
        }
        if (this.useSqlite && this.sqliteDb) {
            try {
                this.sqliteDb.run('DELETE FROM messages WHERE id = ?', [messageId]);
                this.sqliteWriteQueue.push(Date.now());
                return true;
            } catch (e) {
                console.error('sqlite deleteMessage failed', e);
                return false;
            }
        }
        return false;
    }

    // Mark a message as retracted (soft-delete): set message text to a placeholder and status to 'retracted'
    async markMessageRetracted(messageId) {
        if (!messageId) return false;
        const placeholder = '[Pesan ini sudah ditarik]';
        if (this.usePostgres) {
            const client = await this.pgPool.connect();
            try {
                await client.query('UPDATE messages SET message = $1, status = $2 WHERE id = $3', [placeholder, 'retracted', messageId]);
                return true;
            } finally { client.release(); }
        }
        if (this.useSqlite && this.sqliteDb) {
            try {
                this.sqliteDb.run('UPDATE messages SET message = ?, status = ? WHERE id = ?', [placeholder, 'retracted', messageId]);
                this.sqliteWriteQueue.push(Date.now());
                return true;
            } catch (e) {
                console.error('sqlite markMessageRetracted failed', e);
                return false;
            }
        }
        return false;
    }

    // TOTP helpers
    async generateTOTPSecret(userId) {
        const secret = speakeasy.generateSecret({ length: 20 });
        const base32 = secret.base32;
        if (this.usePostgres) {
            const client = await this.pgPool.connect();
            try { await client.query('UPDATE users SET totpsecret = $1 WHERE id = $2', [base32, userId]); } finally { client.release(); }
        } else if (this.useSqlite && this.sqliteDb) {
            try { this.sqliteDb.run('UPDATE users SET totpSecret = ? WHERE id = ?', [base32, userId]); this.sqliteWriteQueue.push(Date.now()); } catch (e) {}
        }
        return { base32, otpauth_url: secret.otpauth_url };
    }

    async verifyTOTP(userId, token) {
        const user = await this.getUserById(userId);
        const secret = user && (user.totpSecret || user.totpsecret || null);
        if (!secret) return false;
        return speakeasy.totp.verify({ secret: secret, encoding: 'base32', token: token, window: 1 });
    }

    // Return undelivered messages for a specific user across all chats
    async getUndeliveredMessages(userId) {
        if (this.usePostgres) {
            const client = await this.pgPool.connect();
            try {
                const res = await client.query('SELECT * FROM messages WHERE toid = $1 AND (status IS NULL OR status != $2) ORDER BY timestamp ASC', [userId, 'delivered']);
                return res.rows;
            } finally { client.release(); }
        }
        if (this.useSqlite && this.sqliteDb) {
            const res = this.sqliteDb.exec('SELECT id,chatid,fromid,toid,message,timestamp,encrypted,status FROM messages WHERE toid = ? AND (status IS NULL OR status != ?) ORDER BY timestamp ASC', [userId, 'delivered']);
            if (!res || !res[0]) return [];
            const cols = res[0].columns;
            return res[0].values.map(r => {
                const o = {};
                for (let i = 0; i < cols.length; i++) o[cols[i]] = r[i];
                return o;
            });
        }

    // JSON fallback removed. If not using a supported DB, return empty
    return [];
    }

    async updateProfile(userId, profileData) {
        console.log('Updating profile for user:', userId);
    // Accept legacy or misspelled 'preference' key from clients and normalize to 'preferences'
    try {
        if (profileData && profileData.preference !== undefined && profileData.preferences === undefined) {
            profileData.preferences = profileData.preference;
            delete profileData.preference;
        }
    } catch (e) {}

    const updatableFields = ['fullName','username','bio','phoneNumber','location','gender','birthDate','profileImageUrl','profileVisibility','preferences'];
        // If caller provided preferences as an object, persist as JSON text
        try {
            if (profileData && profileData.preferences !== undefined && typeof profileData.preferences !== 'string') {
                profileData.preferences = JSON.stringify(profileData.preferences);
            }
        } catch (e) {}

        if (this.usePostgres) {
            const sets = []; const args = []; let idx = 1;
            for (const f of updatableFields) {
                if (profileData[f] !== undefined) { sets.push(`${f} = $${idx++}`); args.push(profileData[f]); }
            }
            if (!sets.length) return await this.getUserById(userId);
            args.push(userId);
            const client = await this.pgPool.connect();
            try {
                await client.query(`UPDATE users SET ${sets.join(', ')} WHERE id = $${idx}`, args);
            } finally { client.release(); }
            return await this.getUserById(userId);
        }
        if (this.useSqlite && this.sqliteDb) {
            const sets = []; const args = [];
            for (const f of updatableFields) {
                if (profileData[f] !== undefined) { sets.push(`${f} = ?`); args.push(profileData[f]); }
            }
            if (!sets.length) return await this.getUserById(userId);
            args.push(userId);
            const q = `UPDATE users SET ${sets.join(', ')} WHERE id = ?`;
            this.sqliteDb.run(q, args);
            this.sqliteWriteQueue.push(Date.now());
            return await this.getUserById(userId);
        }
        throw new Error('No supported DB backend');
    }

    // Change password with simple verification (returns true on success)
    async changePassword(userId, oldPassword, newPassword) {
        if (this.usePostgres) {
            const client = await this.pgPool.connect();
            try {
                const r = await client.query('SELECT password FROM users WHERE id = $1 LIMIT 1', [userId]);
                if (!r.rows || !r.rows.length) throw new Error('User not found');
                const current = r.rows[0].password || '';
                const ok = current ? await bcrypt.compare(oldPassword || '', current) : false;
                if (oldPassword && !ok) throw new Error('Old password does not match');
                const hashed = await bcrypt.hash(newPassword, 10);
                await client.query('UPDATE users SET password = $1 WHERE id = $2', [hashed, userId]);
                return true;
            } finally { client.release(); }
        }
        if (this.useSqlite && this.sqliteDb) {
            try {
                const res = this.sqliteDb.exec('SELECT password FROM users WHERE id = ? LIMIT 1', [userId]);
                if (!res || !res[0] || !res[0].values.length) throw new Error('User not found');
                const current = res[0].values[0][0] || '';
                const ok = current ? await bcrypt.compare(oldPassword || '', current) : false;
                if (oldPassword && !ok) throw new Error('Old password does not match');
                const hashed = await bcrypt.hash(newPassword, 10);
                this.sqliteDb.run('UPDATE users SET password = ? WHERE id = ?', [hashed, userId]);
                this.sqliteWriteQueue.push(Date.now());
                return true;
            } catch (e) {
                throw e;
            }
        }
        throw new Error('No supported DB backend');
    }

    // Delete a user and optionally related messages
    async deleteUser(userId) {
        if (this.usePostgres) {
            const client = await this.pgPool.connect();
            try {
                await client.query('DELETE FROM messages WHERE fromid = $1 OR toid = $1', [userId]);
                await client.query('DELETE FROM users WHERE id = $1', [userId]);
                return true;
            } finally { client.release(); }
        }
        if (this.useSqlite && this.sqliteDb) {
            try {
                this.sqliteDb.run('DELETE FROM messages WHERE fromid = ? OR toid = ?', [userId, userId]);
                this.sqliteDb.run('DELETE FROM users WHERE id = ?', [userId]);
                this.sqliteWriteQueue.push(Date.now());
                return true;
            } catch (e) {
                console.error('deleteUser sqlite failed', e);
                return false;
            }
        }
        return false;
    }

    // One-time migration: import existing JSON data into Postgres if present.
    async _fileExists(p) {
        try {
            await fs.access(p);
            return true;
        } catch (e) {
            return false;
        }
    }

    async _migrateJsonToPostgres() {
        if (!this.usePostgres || !this.pgPool) return;
        // If flag file exists, skip
        if (await this._fileExists(this._migrationFlagPath)) {
            console.log('Postgres migration flag found, skipping migration');
            return;
        }

        // If legacy JSON files exist, try to read them directly via fs
        let users = null;
        let messages = null;
        try {
            const usersPath = path.join(__dirname, 'users.json');
            const messagesPath = path.join(__dirname, 'messages.json');
            if (await this._fileExists(usersPath)) {
                const raw = await fs.readFile(usersPath, 'utf8');
                users = JSON.parse(raw);
            }
            if (await this._fileExists(messagesPath)) {
                const raw = await fs.readFile(messagesPath, 'utf8');
                messages = JSON.parse(raw);
            }
        } catch (e) { users = null; messages = null; }
        const client = await this.pgPool.connect();
        try {
            await client.query('BEGIN');

            if (users && users.users) {
                const upserts = Object.values(users.users).map(u => {
                    return client.query(
                        `INSERT INTO users(id,email,password,username,publickey,lastseen) VALUES($1,$2,$3,$4,$5,$6)
                         ON CONFLICT (id) DO UPDATE SET email = EXCLUDED.email, username = EXCLUDED.username, publickey = EXCLUDED.publickey, lastseen = EXCLUDED.lastseen`,
                        [u.id, u.email || null, u.password || null, u.username || null, u.publicKey || null, u.lastSeen || null]
                    );
                });
                await Promise.all(upserts);
            }

            if (messages) {
                const inserts = [];
                for (const chatId of Object.keys(messages)) {
                    const arr = messages[chatId] || [];
                    for (const m of arr) {
                        // ensure required fields exist
                        const id = m.id || `msg_${Date.now()}_${Math.floor(Math.random()*1000)}`;
                        inserts.push(client.query(
                            'INSERT INTO messages(id,chatid,fromid,toid,message,timestamp,encrypted,status) VALUES($1,$2,$3,$4,$5,$6,$7,$8) ON CONFLICT (id) DO NOTHING',
                            [id, chatId, m.fromId || null, m.toId || null, m.message || null, m.timestamp || new Date().toISOString(), !!m.encrypted, m.status || 'sent']
                        ));
                    }
                }
                await Promise.all(inserts);
            }

            await client.query('COMMIT');
            // write migration flag file
            await fs.writeFile(this._migrationFlagPath, new Date().toISOString());
            console.log('Migration to Postgres completed and flag created');
        } catch (e) {
            try { await client.query('ROLLBACK'); } catch (er) {}
            throw e;
        } finally { client.release(); }
    }

    // Flush queued sqlite writes to disk (async)
    async _flushSqliteWrites() {
        if (!this.useSqlite || !this.sqliteDb) return;
        if (this._sqliteFlushInProgress) return;
        if (!this.sqliteWriteQueue.length) return;
        this._sqliteFlushInProgress = true;
        try {
            const data = this.sqliteDb.export();
            await fs.writeFile(this._sqliteFilePath, Buffer.from(data));
            // clear queue
            this.sqliteWriteQueue.length = 0;
        } catch (e) {
            console.error('Failed to flush sqlite writes', e);
        } finally {
            this._sqliteFlushInProgress = false;
        }
    }

    // Synchronous flush used on process exit (best-effort)
    _flushSqliteWritesSync() {
        try {
            if (!this.useSqlite || !this.sqliteDb) return;
            // export returns a typed array
            const data = this.sqliteDb.export();
            require('fs').writeFileSync(this._sqliteFilePath, Buffer.from(data));
            this.sqliteWriteQueue.length = 0;
        } catch (e) {
            console.error('Failed to sync-flush sqlite DB on exit', e);
        }
    }

    // Normalize raw DB user object (Postgres row or sqlite row) into API-friendly shape
    _normalizeUserObject(raw) {
        if (!raw) return null;
        // normalize field names and provide safe defaults
        const user = {
            id: raw.id || raw.userId || '',
            username: raw.username || raw.name || '',
            email: raw.email || '',
            publicKey: raw.publickey || raw.publicKey || null,
            lastSeen: raw.lastseen || raw.lastSeen || 'offline',
            fullName: raw.fullName || raw.fullname || null,
            bio: raw.bio || null,
            phoneNumber: raw.phoneNumber || raw.phonenumber || null,
            location: raw.location || null,
            gender: raw.gender || null,
            birthDate: raw.birthDate || raw.birthdate || null,
            profileImageUrl: raw.profileImageUrl || raw.profileimageurl || null,
            profileVisibility: raw.profileVisibility || raw.profilevisibility || raw.profileVisibility || raw.profilevisibility || 'public',
            contacts: (() => {
                try {
                    if (!raw.contacts) return [];
                    if (Array.isArray(raw.contacts)) return raw.contacts;
                    if (typeof raw.contacts === 'string') return JSON.parse(raw.contacts);
                    return [];
                } catch (e) { return []; }
            })(),
            color: raw.color || 0xFF2196F3,
            preferences: (() => {
                try {
                    if (!raw.preferences) return {};
                    if (typeof raw.preferences === 'string') return JSON.parse(raw.preferences);
                    return raw.preferences;
                } catch (e) { return {}; }
            })(),
        };
        return user;
    }
}

module.exports = DatabaseHandler;
