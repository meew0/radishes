#!/usr/bin/env ruby
# frozen_string_literal: true
#
# music_sorter.rb – Single‑file Sinatra web‑app for assigning custom title‑sort orders to tracks
# v2.0 – album picker now skips albums that are already fully completed. We achieve this by
#        attaching the *output* database to the main connection so both tables can be joined.
#        Existing sort‑order assignments are kept intact.
#
# Usage:
#   ruby music_sorter.rb INPUT_DB_PATH OUTPUT_DB_PATH
# (defaults to ./music.db and ./sort_orders.db)
#
# Dependencies: sinatra, sqlite3
# -----------------------------------------------------------------------------
require 'sinatra'
require 'sqlite3'
require 'json'
require 'erb'
require 'thread'

set :bind, '0.0.0.0'
set :port, (ENV['PORT'] || 4567)
set :static, false

# ----------------------------------------------------------------------------
# Database setup – we use *one* connection and ATTACH the output DB so we can
# perform cross‑database queries (read+write). This preserves existing data.
# ----------------------------------------------------------------------------
INPUT_DB  = (ARGV[0] || ENV['INPUT_DB']  || 'music.db').freeze
OUTPUT_DB = (ARGV[1] || ENV['OUTPUT_DB'] || 'sort_orders.db').freeze

begin
  DB = SQLite3::Database.new(INPUT_DB)
  DB.results_as_hash = true
  # Attach (or create) the output DB under the schema name `sortdb`
  DB.execute('ATTACH DATABASE ? AS sortdb', OUTPUT_DB)
  DB.execute <<~SQL
    CREATE TABLE IF NOT EXISTS sortdb.title_sort_order (
      media_file_id TEXT PRIMARY KEY,
      sort_order    TEXT NOT NULL
    );
  SQL
rescue SQLite3::Exception => e
  abort "Database error: #{e.message}"
end

# Mutex to serialize writes when threaded (Sinatra in threaded mode)
WRITE_LOCK = Mutex.new

helpers do
  include Rack::Utils
  alias_method :h, :escape_html

  # Return stored sort order for given media_file id, or nil
  def sort_order_for_id(id)
    row = DB.get_first_row('SELECT sort_order FROM sortdb.title_sort_order WHERE media_file_id = ?', id)
    row && row['sort_order']
  end

  # Heuristic pre‑fill: use sort order of any other track sharing the same title
  def sort_order_for_title(title)
    ids = DB.execute('SELECT id FROM media_file WHERE title = ? LIMIT 100', title).map { |r| r['id'] }
    return nil if ids.empty?
    q = 'SELECT sort_order FROM sortdb.title_sort_order WHERE media_file_id = ? LIMIT 1'
    ids.each do |mid|
      row = DB.get_first_row(q, mid)
      return row['sort_order'] if row
    end
    nil
  end

  # Insert/update many records in one transaction (expects [[id, order], ...])
  def upsert_many(pairs)
    DB.transaction do
      pairs.each do |mid, order|
        DB.execute(<<~SQL, [mid, order])
          INSERT INTO sortdb.title_sort_order (media_file_id, sort_order)
               VALUES (?, ?)
               ON CONFLICT(media_file_id)
               DO UPDATE SET sort_order = excluded.sort_order;
        SQL
      end
    end
  end
end

# ----------------------------------------------------------------------------
# Routes
# ----------------------------------------------------------------------------
get '/' do
  # Pick a random album that still has *at least one* track without sort order.
  # We do this entirely in SQL, thanks to the attached DB.
  @album = DB.get_first_row <<~SQL
    SELECT * FROM album a
    WHERE EXISTS (
      SELECT 1 FROM media_file mf
      LEFT JOIN sortdb.title_sort_order tso ON tso.media_file_id = mf.id
      WHERE mf.album_id = a.id AND tso.media_file_id IS NULL
    )
    ORDER BY RANDOM()
    LIMIT 1;
  SQL
  halt 404, 'All albums are fully processed! 🎉' unless @album

  # Fetch tracks for that album (with deterministic ordering)
  @tracks = DB.execute(<<~SQL, @album['id'])
    SELECT * FROM media_file
     WHERE album_id = ?
  ORDER BY disc_number, track_number;
  SQL

  # Enrich track hashes with stored / suggested sort orders
  @tracks = @tracks.map do |t|
    stored    = sort_order_for_id(t['id'])
    suggested = stored || sort_order_for_title(t['title']) || ''
    t.merge('sort_order' => suggested)
  end

  erb :album_form
end

# AJAX endpoint – store/update a single sort order (for live typing)
post '/update' do
  data = JSON.parse(request.body.read) rescue halt(400, 'Invalid JSON')
  id    = (data['id'] || '').strip
  order = (data['sort_order'] || '').strip
  halt 422, 'Missing id' if id.empty?

  WRITE_LOCK.synchronize { upsert_many([[id, order]]) }
  content_type :json
  { status: 'ok' }.to_json
end

# AJAX endpoint – batch upsert { items: [ {id: .., sort_order: ..}, ... ] }
post '/batch' do
  data = JSON.parse(request.body.read) rescue halt(400, 'Invalid JSON')
  items = data['items']
  halt 400, 'items must be an array' unless items.is_a?(Array)

  pairs = items.map { |it| [(it['id'] || '').strip, (it['sort_order'] || '').strip] }
  halt 422, 'Blank id in batch' if pairs.any? { |mid, _| mid.empty? }

  WRITE_LOCK.synchronize { upsert_many(pairs) }
  content_type :json
  { status: 'ok', updated: pairs.length }.to_json
end

# ----------------------------------------------------------------------------
# Inline template (unchanged except for small tweaks)
# ----------------------------------------------------------------------------
__END__

@@ album_form
<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Track sort‑order annotator</title>
  <style>
    body { font-family: system-ui, sans-serif; margin: 2rem calc(max(40% - 10rem, 2rem)); }
    table { border-collapse: collapse; width: 100%; margin-bottom: 1rem; }
    th, td { padding: 0.4rem 0.6rem; border: 1px solid #ddd; }
    th { background: #f5f5f5; text-align: left; }
    tr:nth-child(even) { background: #fafafa; }
    input[type=text] { width: 100%; box-sizing: border-box; padding: 0.3rem; }
    .small { font-size: 0.9rem; color: #666; }
    button { padding: 0.55rem 1.1rem; font-size: 1rem; cursor: pointer; }
  </style>
</head>
<body>
  <h2>
    <%= h @album['name'] %>
    <% if @album['album_artist'] && !@album['album_artist'].empty? %>
      <span class="small">— <%= h @album['album_artist'] %></span>
    <% elsif @album['artist'] && !@album['artist'].empty? %>
      <span class="small">— <%= h @album['artist'] %></span>
    <% end %>
  </h2>

  <table>
    <thead>
      <tr>
        <th style="width:4ch">#</th>
        <th>Title</th>
        <th style="width:18rem">Sort order</th>
      </tr>
    </thead>
    <tbody>
      <% @tracks.each_with_index do |t, idx| %>
        <tr>
          <td><%= t['track_number'] %></td>
          <td><%= h t['title'] %></td>
          <td>
            <input type="text"
                   class="sort-order"
                   data-id="<%= t['id'] %>"
                   data-idx="<%= idx %>"
                   data-title="<%= h t['title'] %>"
                   value="<%= h t['sort_order'] %>"
                   autocomplete="off">
          </td>
        </tr>
      <% end %>
    </tbody>
  </table>

  <button id="next-btn">Next incomplete album ⟳</button>

<script>
(() => {
  const inputs = Array.from(document.querySelectorAll('input.sort-order'));

  const saveSingle = (inp) => {
    fetch('/update', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ id: inp.dataset.id, sort_order: inp.value })
    }).catch(() => console.warn('Save failed'));
  };

  const ensureFilled = (inp) => {
    if (inp.value.trim() === '') inp.value = inp.dataset.title;
  };

  const propagateSimilar = (source) => {
    const baseTitle = source.dataset.title;
    const entered   = source.value.trim();
    if (!entered) return;

    inputs.forEach((target) => {
      if (target === source) return;
      const title = target.dataset.title;
      if (!title.startsWith(baseTitle)) return;
      if (target.value.trim() === '' || target.value.trim() === title) {
        const suffix = title.slice(baseTitle.length);
        target.value = entered + suffix;
        saveSingle(target);
      }
    });
  };

  const bulkSave = () => {
    const items = inputs.map(inp => ({ id: inp.dataset.id, sort_order: inp.value.trim() === '' ? inp.dataset.title : inp.value }));
    return fetch('/batch', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ items })
    }).then(r => {
      if (!r.ok) throw new Error('batch failed');
      return r.json();
    });
  };

  inputs.forEach((inp, i) => {
    const handleEdit = () => { ensureFilled(inp); propagateSimilar(inp); saveSingle(inp); };
    inp.addEventListener('change', handleEdit);
    inp.addEventListener('blur',   handleEdit);

    inp.addEventListener('keydown', (e) => {
      if (['Enter', 'ArrowDown', 'ArrowUp'].includes(e.key)) {
        e.preventDefault();
        ensureFilled(inp); propagateSimilar(inp);
        if (e.key === 'Enter' && i === inputs.length - 1) {
          bulkSave().then(() => window.location.reload());
          return;
        }
        const targetIndex = (e.key === 'ArrowUp') ? i - 1 : i + 1;
        const target = inputs[targetIndex];
        if (target) target.focus();
      }
    });
  });

  if (inputs[0]) inputs[0].focus();

  document.getElementById('next-btn').addEventListener('click', () => {
    inputs.forEach(ensureFilled);
    bulkSave().then(() => window.location.reload());
  });
})();
</script>
</body>
</html>
