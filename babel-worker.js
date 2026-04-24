// babel-worker.js — compiles JSX on a background thread
importScripts('https://unpkg.com/@babel/standalone/babel.min.js');

self.onmessage = function(e) {
  const source = e.data;
  try {
    const result = Babel.transform(source, {
      presets: ['react'],
      compact: false,
    });
    self.postMessage({ ok: true, code: result.code });
  } catch(err) {
    self.postMessage({ ok: false, error: err.message + ' (line ' + (err.loc && err.loc.line) + ')' });
  }
};
