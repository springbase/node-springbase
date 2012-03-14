/*
 * Copyright (c) 2012, Springbase
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice, this
 *    list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR
 * ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

var exports,
    https = require('https'),
    EventEmitter = require('events').EventEmitter,
    Query,
    Table,
    Reader,
    __hasProp = Object.prototype.hasOwnProperty,
    __extends = function(child, parent) { for (var key in parent) { if (__hasProp.call(parent, key)) { child[key] = parent[key]; } } function ctor() { this.constructor = child; } ctor.prototype = parent.prototype; child.prototype = new ctor; child.__super__ = parent.prototype; return child; },
    __override = function(defaults, overrides) { for (var key in overrides) { if (__hasProp.call(overrides, key)) { defaults[key] = overrides[key]; } } return defaults; },
    __requireProps = function(obj, props) { for (var i = 0; i < props.length; i++) { var prop = props[i]; if (!__hasProp.call(obj, prop)) { throw new Error("Missing required property: " + prop); } } };

(function() {

    exports.data = {};

    exports.data.Connection = (function() {

        __extends(Connection, EventEmitter);

        function Connection(config) {
            __override(this, {
                server: "springbase.com",
                port: 443
            });
            __override(this, config);
            __requireProps(this, ["server", "port", "username", "password", "application"]);
            this._authHeaders = {
                "X-Springbase-User": this.username,
                "X-Springbase-Password": this.password,
                "X-Springbase-Application": this.application
            };
            this._requestConfig = {
                host: this.server,
                port: this.port,
                headers: this._authHeaders
            };
        }

        Connection.prototype.createRequest = function(config) {
            var finalConfig = __override(__override({}, this._requestConfig), config);
            if (typeof config.data !== "undefined") {
                finalConfig.headers["Content-Length"] = config.data.length;
            } else {
                delete finalConfig.headers["Content-Length"];
            }
            var req = https.request(finalConfig);
            req.on('error', function(err) {
                console.error('Https error', err, "Config was:", finalConfig);
            });
            if (config.data) {
                req.write(config.data);
            }
            return req;
        };

        Connection.prototype.doRequest = function(config, callback) {
            var self = this,
                request = Connection.prototype.createRequest.apply(this, arguments);
            request.end();
            request.on('response', function(response) {
                var body = '';
                response.on('data', function(chunk) {
                    body += chunk;
                });
                response.on('end', function () {
                    if (config.success) {
                        config.success.call(config.context || self, body);
                    }
                });
            });
        };

        Connection.prototype.getQuery = function(queryName, callback) {
            var self = this,
                query = new Query({ connection: this });
            this.doRequest({
                path: "/springbase/node-driver/applications/"
                    + this.application
                    + "/queries/" + queryName,
                method: "GET",
                success: function(body) {
                    try {
                        __override(query, JSON.parse(body));
                    } catch(e) {
                        throw new Error("Invalid response from server.");
                    }
                    query.ready = true;
                    if (callback) {
                        callback.call();
                    }
                    query.emit("ready");
                }
            });
            return query;
        };

        Connection.prototype.getTable = function(tableName, callback) {
            var self = this,
                table = new Table({ name: tableName, connection: this });
            this.doRequest({
                path: "/springbase/node-driver/applications/"
                    + this.application
                    + "/tables/" + tableName,
                method: "GET",
                success: function(body) {
                    try {
                        __override(table, JSON.parse(body));
                    } catch(e) {
                        throw new Error("Invalid response from server.");
                    }
                    table._ready = true;
                    if (callback) {
                        callback.call();
                    }
                    table.emit("ready");
                }
            });
            return table;
        };

        return Connection;

    })();

    Query = exports.data.Query = (function() {

        __extends(Query, EventEmitter);

        function Query(config) {
            __override(this, {
                ready: false,
                start: 0,
                limit: 500
            });
            __override(this, config);
        }

        function requireReady(query) {
            if (!query.ready) {
                throw new Error("Query still loading; wait for query's \"ready\" event before using the query.");
            }
        }

        function formatParams(parameters) {
            var paramList = [];
            for (var key in parameters) {
                if (__hasProp.call(parameters, key)) {
                    var val = parameters[key];
                    if (val instanceof Date) {
                        val = toTimestamp(val);
                    } else {
                        val = JSON.stringify(val);
                    }
                    paramList.push(key.toLowerCase() + "=" + val);
                }
            }
            return paramList;
        }

        Query.prototype.execute = function(parameters, callback) {
            if (this.type === 0) {
                return this.openReader.apply(this, arguments);
            }
            return this.executeNonSelect.apply(this, arguments);
        };

        Query.prototype.executeNonSelect = function(parameters, callback) {
            requireReady(this);
            if (arguments.length === 0) {
                parameters = {};
            } else if (arguments.length === 1 && typeof arguments[0] === "function") {
                callback = parameters;
                parameters = {};
            }
            var self = this;
            var allparams = { up: parameters };
            this.connection.doRequest({
                path: "/springbase/node-driver/applications/"
                    + this.connection.application
                    + "/queries/" + this.id + "?p=" + JSON.stringify(allparams, dateReplacer),
                method: "POST",
                data: "",
                success: function(body) {
                    var numRecordsAffected = parseInt(body, 10);
                    if (callback) {
                        callback.call(this, numRecordsAffected);
                    }
                }
            });
        };

        Query.prototype.openReader = function(parameters, callback) {
            requireReady(this);
            if (arguments.length === 0) {
                parameters = {};
            } else if (arguments.length === 1 && typeof arguments[0] === "function") {
                callback = parameters;
                parameters = {};
            }
            var self = this,
                reader = new Reader();
            var allparams = { start: this.start, limit: this.limit, up: parameters };
            this.connection.doRequest({
                path: "/springbase/node-driver/applications/"
                    + this.connection.application
                    + "/queries/" + this.id
                    + "/results?p=" + JSON.stringify(allparams, dateReplacer),
                method: "GET",
                success: function(body) {
                    var feed;
                    try {
                        feed = JSON.parse(body);
                    } catch(e) {
                        throw new Error("Invalid response from server.");
                    }
                    reader._load(feed, callback);
                }
            });
            return reader;
        };

        return Query;

    })();

    Reader = exports.data.Reader = (function() {

        __extends(Reader, EventEmitter);
        var _NOT_LOADED_ERROR = new Error("Reader not loaded; wait for reader's \"ready\" event before using the reader.");

        function Reader() {
            __override(this, {
                _pos: -1,
                _ready: false
            });
        }

        function transformRow(dataRow) {
            var out = {},
                columns = this._columns;
            for (var i = 0; i < columns.length; i++) {
                var data = dataRow[i];
                if (columns[i].type === 2) {
                    data = new Date(parseInt(data, 10));
                }
                out[columns[i].name] = data;
            }
            return out;
        }

        Reader.prototype._load = function(feed, callback) {
            var self = this;
            __override(this, {
                _columns: feed.columns,
                _data: feed.rows,
                length: feed.rows.length,
                _ready: true
            });
            this.emit("ready");
            if (callback) {
                callback.call();
            }
            if (this.listeners("row").length > 0) {
                this._data.map(function(dataRow) {
                    self.emit("row", transformRow.call(self, dataRow));
                });
            }
        };

        Reader.prototype.read = function() {
            if (!this._ready) {
                throw _NOT_LOADED_ERROR;
            }
            if (this._pos >= this.length - 1) {
                return null;
            }
            this._pos++;
            return transformRow.call(this, this._data[this._pos]);
        };

        Reader.prototype.readAll = function() {
            var self = this;
            if (!this._ready) {
                throw _NOT_LOADED_ERROR;
            }
            return this._data.map(function(dataRow) {
                return transformRow.call(self, dataRow);
            });
        };

        return Reader;

    })();

    Table = exports.data.Table = (function() {

        var _NOT_LOADED_ERROR = new Error("Table not loaded; wait for table's \"ready\" event before using the table.");

        __extends(Table, EventEmitter);

        function Table(config) {
            __override(this, {
                _ready: false,
                lastInsertedRowID: null
            });
            __override(this, config);
        }

        function requireReady() {
            if (!this._ready) {
                throw _NOT_LOADED_ERROR;
            }
        }

        Table.prototype.insertRow = function(row, callback) {
            if (Array.isArray(row)) {
                throw new Error("Table.insertRow requires a single row.");
            }
            return Table.prototype.insertRows.apply(this, arguments);
        };

        Table.prototype.insertRows = function(rows, callback) {
            requireReady.call(this);
            if (!Array.isArray(rows)) {
                rows = [ rows ];
            }
            var self = this;
            this.connection.doRequest({
                path: "/springbase/node-driver/applications/"
                    + this.connection.application
                    + "/tables/" + this.id + "/data",
                method: "POST",
                data: JSON.stringify(rows, dateReplacer),
                success: function(body) {
                    if (callback) {
                        callback.call();
                    }
                }
            });
        };

        Table.prototype.updateRow = function(rowID, values, callback) {
            requireReady.call(this);
            var self = this;
            var requestBody = {
                id: rowID,
                values: values
            };
            this.connection.doRequest({
                path: "/springbase/node-driver/applications/"
                    + this.connection.application
                    + "/tables/" + this.id + "/data",
                method: "PUT",
                data: JSON.stringify(requestBody, dateReplacer),
                success: function (body) {
                    if (callback) {
                        callback.call();
                    }
                }
            });
        };

        Table.prototype.deleteRows = function(rowIDs, callback) {
            requireReady.call(this);
            if (!Array.isArray(rowIDs)) {
                rowIDs = [ rowIDs ];
            }
            var self = this;
            this.connection.doRequest({
                path: "/springbase/node-driver/applications/"
                    + this.connection.application
                    + "/tables/" + this.id + "/data",
                method: "DELETE",
                data: JSON.stringify(rowIDs),
                success: function (body) {
                    if (callback) {
                        callback.call();
                    }
                }
            });
        };

        Table.prototype.openReader = function(parameters, callback) {
            requireReady.call(this);
            if (arguments.length === 1 && typeof arguments[0] === "function") {
                callback = parameters;
                parameters = {};
            }
            var self = this,
                reader = new Reader();
            var paramList = [];
            paramList.push("start=" + (parameters.start || 0));
            paramList.push("limit=" + (parameters.limit || 500));
            this.connection.doRequest({
                path: "/springbase/node-driver/applications/"
                    + this.connection.application
                    + "/tables/" + this.id + "/data?"
                    + paramList.join("&"),
                method: "GET",
                success: function(body) {
                    var feed;
                    try {
                        feed = JSON.parse(body);
                    } catch(e) {
                        throw new Error("Invalid response from server.");
                    }
                    reader._load(feed, callback);
                }
            });
            return reader;
        };

        return Table;

    })();

    function dateReplacer(key, value) {
        if (this[key] instanceof Date) {
            var date = this[key],
                utc = new Date(date.getUTCFullYear(), date.getUTCMonth(), date.getUTCDate(),  date.getUTCHours(), date.getUTCMinutes(), date.getUTCSeconds(), date.getUTCMilliseconds());
            return utc.getTime().toString();
        }
        return value;
    }
})();