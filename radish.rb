require 'taglib'
require 'fileutils'
require 'tomlrb'
require 'date'
require 'json'
require 'uri'
require 'open-uri'
require 'cgi'

# radish should manage:
# - album artist
# - track numbers
# - album & artist & AA sort order
# - cover art in folder
# - release date & doujin events
# - folder & file names:
#     [date/event] album artist - album [format]
# - rhythmdb integration

config_file_path = File.join(File.expand_path(File.dirname(__FILE__)), 'radish.toml')
config = Tomlrb.load_file(config_file_path)

# Defines cl, hr2, etc
load config['utils']

def cont_range?(array)
  !array.include?(nil) && array.min == 1 && array.max == array.length && array.sort == 1.upto(array.length).to_a
end

def date_str?(str)
  return false if str.nil?
  str.match?(/^\d{4}-\d{1,2}-\d{1,2}$/)
end

def parse_date(str, sep)
  year, month, day = str.split(sep).map(&:to_i)
  Date.new(year, month, day)
end

def parse_event_date(str); parse_date(str, '.'); end
def parse_release_date(str); parse_date(str, '-'); end

def load_event_file(config)
  dates_by_event, events_by_date = {}, {}

  unless config['events_file'].nil?
    event_lines = File.read(config['events_file']).lines.reject { |e| e.strip.empty? || e.start_with?('#') }
    event_lines.each do |e|
      name, dates_str = e.split(': ')
      if dates_str.include?('-')
        start_date, end_date = dates_str.split('-').map { |e| parse_event_date(e) }
        dates = start_date.upto(end_date).to_a
      else
        dates = [parse_event_date(dates_str)]
      end

      # Use the last day of the event
      dates_by_event[name] = dates.last

      dates.each do |date|
        events_by_date[date] ||= []
        events_by_date[date] << name
      end
    end
  end

  cl 32, 'Loaded ', dates_by_event.length, ' events with ', events_by_date.length, ' total dates'
  [dates_by_event, events_by_date]
end

dates_by_event, events_by_date = load_event_file(config)

def query
  STDIN.gets.chomp
end

def spacey(o)
  (o.nil? || o == '') ? ' ' : o
end

def deslash(o)
  o.nil? ? nil : o.gsub('/', '／')
end

def pq(prompt, default = nil)
  prompt = prompt.sub('%%', spacey(default))
  split = prompt.split(/(?<=\[)|(?=\])/)
  print split.map.with_index { |e, i| i % 2 == 0 ? e.c(90) : e.c(37) }.join
  result = query
  return nil if result == '-' || (result.empty? && !default.nil? && default == '')
  return default if result.empty?
  result
end

def ffprobe_stream_property(file, property, stream_descriptor = 'a:0')
  safe_run('ffprobe', '-v', 'error', '-pattern_type', 'none', '-select_streams', stream_descriptor, '-show_entries', 'stream=' + property, '-of', 'default=noprint_wrappers=1:nokey=1', file, no_stderr: true).strip
end

def format_str(codec_name, bit_rate, bit_depth, sample_rate)
  case codec_name
  when 'flac'
    return 'FLAC' if bit_depth == '16'
    sr_short = (sample_rate.to_i / 1000.0).to_s
    sr_short = sr_short[0..-3] if sr_short.end_with?('.0')
    "FLAC #{bit_depth}-#{sr_short}"
  when 'mp3'
    return 'MP3' unless bit_rate.end_with?('000')
    br_short = bit_rate[0..-4]
    "MP3 #{br_short}"
  when 'vorbis'
    br_short = (bit_rate.to_i / 1000.0).round
    "Vorbis #{br_short}"
  when 'opus'
    'Opus'
  when 'aac'
    'AAC'
  else
    cl 33, 'Unknown codec name: ', codec_name
    nil
  end
end

ALBUMARTISTSORT = 'ALBUMARTISTSORT'

def hash_downcase(hash)
  Hash[hash.map { |k, v| [k.downcase, v] }]
end

def mp3_unpack(str)
  return nil if str.nil?
  return str.split('/').map(&:to_i) if str.include?('/')
  [str.to_i, nil]
end

def mp3_user_value(tag, desc)
  xxx_frames = tag.frame_list('TXXX')
  frame = xxx_frames.find { |e| e.description == desc }
  return nil if frame.nil?
  frame.field_list[1]
end

def read_tags_from_xiph(tag)
  tags = {}
  tags[:album] = tag.album
  tags[:artist] = tag.artist
  fields = hash_downcase(tag.field_list_map)
  tags[:release_date] = fields['date']&.first || (tag.year == 0 ? nil : tag.year.to_s)
  tags[:album_artist] = fields['albumartist']&.first
  tags[:artist_sort] = fields['artistsort']&.first
  tags[:album_sort] = fields['albumsort']&.first
  tags[:album_artist_sort] = fields['albumartistsort']&.first
  tags[:track] = fields['tracknumber']&.first&.to_i || fields['track']&.first&.to_i
  tags[:total_tracks] = fields['totaltracks']&.first&.to_i || fields['tracktotal']&.first&.to_i || fields['trackc']&.first&.to_i
  tags[:disc] = fields['discnumber']&.first&.to_i || fields['disc']&.first&.to_i
  tags[:total_discs] = fields['totaldiscs']&.first&.to_i || fields['disctotal']&.first&.to_i || fields['discc']&.first&.to_i
  tags[:disambiguation] = fields['musicbrainz_albumcomment']&.first
  tags[:mb_release_id] = fields['musicbrainz_albumid']&.first
  tags
end

def read_tags(filename, codec_name)
  tags = {}

  case codec_name
  when 'flac'
    TagLib::FLAC::File.open(filename) do |file|
      id3 = file.id3v1_tag? || file.id3v2_tag?
      xiph = file.xiph_comment?
      if id3 && !xiph
        cl 33, 'FLAC file ', filename, ' has an ID3 tag but no Xiph comment!'
      elsif !xiph
        cl 33, 'FLAC file ', filename, ' has no tags at all!'
      else
        tag = file.xiph_comment
        tags.merge!(read_tags_from_xiph(tag))
      end
    end
  when 'mp3'
    TagLib::MPEG::File.open(filename) do |file|
      if file.id3v2_tag?
        tag = file.id3v2_tag
        tags[:album] = tag.album
        tags[:artist] = tag.artist
        tags[:release_date] = tag.frame_list('TDRC')&.first&.to_s || (tag.year == 0 ? nil : tag.year.to_s)
        tags[:album_artist] = tag.frame_list('TPE2')&.first&.to_s
        tags[:artist_sort] = tag.frame_list('TSOP')&.first&.to_s
        tags[:album_sort] = tag.frame_list('TSOA')&.first&.to_s
        tags[:album_artist_sort] = mp3_user_value(tag, 'ALBUMARTISTSORT')
        tags[:disambiguation] = mp3_user_value(tag, 'MusicBrainz Album Comment')
        tags[:mb_release_id] = mp3_user_value(tag, 'MusicBrainz Album Id')
        tags[:track], tags[:total_tracks] = mp3_unpack(tag.frame_list('TRCK')&.first&.to_s)
        tags[:disc], tags[:total_discs] = mp3_unpack(tag.frame_list('TPOS')&.first&.to_s)
      elsif file.id3v1_tag?
        cl 33, 'File ', filename, ' only has an ID3v1 tag, this is unsupported!'
      else
        cl 33, 'File ', filename, ' has no ID3 tags!'
      end
    end
  when 'aac'
    TagLib::MP4::File.open(filename) do |file|
      tag = file.tag
      tags[:album] = tag.album
      tags[:artist] = tag.artist
      ilm = tag.item_map
      tags[:release_date] = ilm["\xC2\xA9day"]&.to_string_list&.first || (tag.year == 0 ? nil : tag.year.to_s)
      tags[:album_artist] = ilm['aART']&.to_string_list&.first
      tags[:artist_sort] = ilm['soar']&.to_string_list&.first
      tags[:album_sort] = ilm['soal']&.to_string_list&.first
      tags[:album_artist_sort] = ilm['soaa']&.to_string_list&.first
      tags[:track] = ilm['trkn']&.to_int_pair&.first
      tags[:disc] = ilm['disc']&.to_int_pair&.first
    end
  when 'opus'
    # TODO
    cl 33, 'Reading tags from Opus files is not yet supported!'
  when 'vorbis'
    TagLib::Ogg::Vorbis::File.open(filename) do |file|
      tag = file.tag
      tags.merge!(read_tags_from_xiph(tag))
    end
  end

  tags
end

def iterate_tags(tags, &block)
  raise 'total_tracks must be specified' if !tags[:track].nil? && !tags.key?(:total_tracks)
  raise 'total_discs must be specified' if !tags[:disc].nil? && !tags.key?(:total_discs)
  tags.each { |k, v| block.call(k, v) unless v.nil? }
end

def write_tags_to_xiph(tag, tags)
  iterate_tags(tags) do |k, v|
    case k
    when :album
      tag.album = v
    when :artist
      tag.artist = v
    when :release_date
      tag.add_field 'DATE', v.to_s
      tag.year = v.year if v.is_a?(Date) && tags[:year].nil?
    when :year
      tag.year = v
    when :album_artist
      tag.add_field 'ALBUMARTIST', v
      tag.add_field 'ALBUM_ARTIST', v
      tag.add_field 'ALBUM ARTIST', v
    when :artist_sort
      tag.add_field 'ARTISTSORT', v
    when :album_sort
      tag.add_field 'ALBUMSORT', v
    when :album_artist_sort
      tag.add_field 'ALBUMARTISTSORT', v
    when :track
      tag.add_field 'TRACK', v.to_s
      tag.add_field 'TRACKNUMBER', v.to_s
      tag.add_field 'TRACKTOTAL', tags[:total_tracks].to_s
      tag.add_field 'TRACKC', tags[:total_tracks].to_s
      tag.add_field 'TOTALTRACKS', tags[:total_tracks].to_s
    when :disc
      tag.add_field 'DISC', v.to_s
      tag.add_field 'DISCNUMBER', v.to_s
      tag.add_field 'DISCTOTAL', tags[:total_discs].to_s
      tag.add_field 'DISCC', tags[:total_discs].to_s
      tag.add_field 'TOTALDISCS', tags[:total_discs].to_s
    end
    tag.add_field 'DESCRIPTION', ''
    tag.add_field 'COMMENT', ''
  end
end

def mp3_t_frame(tag, key, value)
  frame = TagLib::ID3v2::TextIdentificationFrame.new(key, TagLib::String::UTF8)
  frame.text = value.to_s
  tag.remove_frames(key)
  tag.add_frame(frame)
end

def write_tags(filename, codec_name, tags)
  unless File.exist?(filename)
    cl 33, 'Trying to write tags to non-existent file: ', filename
    return
  end

  case codec_name
  when 'flac'
    TagLib::FLAC::File.open(filename) do |file|
      tag = file.xiph_comment
      write_tags_to_xiph(tag, tags)
      file.save
    end
  when 'mp3'
    TagLib::MPEG::File.open(filename) do |file|
      tag = file.id3v2_tag
      iterate_tags(tags) do |k, v|
        case k
        when :album
          tag.album = v
        when :artist
          tag.artist = v
        when :release_date
          mp3_t_frame(tag, 'TDRC', v)
          tag.year = v.year if v.is_a?(Date) && tags[:year].nil?
        when :year
          tag.year = v
        when :album_artist
          mp3_t_frame(tag, 'TPE2', v)
        when :artist_sort
          mp3_t_frame(tag, 'TSOP', v)
        when :album_sort
          mp3_t_frame(tag, 'TSOA', v)
        when :album_artist_sort
          xxx_frames = tag.frame_list('TXXX')
          frame = xxx_frames.find { |e| e.description == ALBUMARTISTSORT }
          tag.remove_frame(frame) unless frame.nil?
          new_frame = TagLib::ID3v2::UserTextIdentificationFrame.new(ALBUMARTISTSORT, [v.to_s], TagLib::String::UTF8)
          tag.add_frame(new_frame)
        when :track
          mp3_t_frame(tag, 'TRCK', "#{v}/#{tags[:total_tracks]}")
        when :disc
          mp3_t_frame(tag, 'TPOS', "#{v}/#{tags[:total_discs]}")
        end
      end
      tag.remove_frames('COMM')
      file.save
    end
  when 'aac'
    TagLib::MP4::File.open(filename) do |file|
      tag = file.tag
      ilm = tag.item_map
      iterate_tags(tags) do |k, v|
        case k
        when :album
          tag.album = v
        when :artist
          tag.artist = v
        when :release_date
          ilm.insert "\xC2\xA9day", TagLib::MP4::Item.from_string_list([v.to_s])
          tag.year = v.year if v.is_a?(Date) && tags[:year].nil?
        when :year
          tag.year = v
        when :album_artist
          ilm.insert 'aART', TagLib::MP4::Item.from_string_list([v.to_s])
        when :artist_sort
          ilm.insert 'soar', TagLib::MP4::Item.from_string_list([v.to_s])
        when :album_sort
          ilm.insert 'soal', TagLib::MP4::Item.from_string_list([v.to_s])
        when :album_artist_sort
          ilm.insert 'soaa', TagLib::MP4::Item.from_string_list([v.to_s])
        when :track
          ilm.insert 'trkn', TagLib::MP4::Item.from_int_pair([v, tags[:total_tracks]])
        when :disc
          ilm.insert 'disk', TagLib::MP4::Item.from_int_pair([v, tags[:total_discs]])
        end
      end
      ilm.erase "\xC2\xA9cmt"
      file.save
    end
  when 'opus'
    # TODO
    cl 33, 'Writing tags to Opus files is not yet supported! You will have to do the following changes manually:'
    pp tags
  when 'vorbis'
    TagLib::Ogg::Vorbis::File.open(filename) do |file|
      tag = file.tag
      write_tags_to_xiph(tag, tags)
      file.save
    end
  end
end

class ASOSkip < Exception; end

# ASO / artist sort order file
class ASO
  def initialize(path)
    @path = path
    load
  end

  def load
    @aso = File.exist?(@path) ? JSON.parse(File.read(@path)) : {}
  end

  def save
    File.write(@path, JSON.pretty_generate(@aso))
  end

  def ingest(tags_set)
    begin
      tags_set.each do |tags|
        ingest_one_manual(tags[:artist], tags[:artist_sort])
        ingest_one_manual(tags[:album_artist], tags[:album_artist_sort])
      end
    rescue ASOSkip
      cl 33, 'Skipping remaining ASO ingestions.'
    end
  end

  def ingest_one_manual(artist, sort)
    return if artist.nil?

    if @aso[artist].nil? && (sort.nil? || artist == sort)
      query_new(artist)
    elsif @aso[artist].nil? && artist != sort
      ckn 36, 'Storing sort order [', 37, sort, 36, '] for artist [', 37, artist, 36, '], or enter alternative: '
      command = query_stdin
      if command.empty?
        @aso[artist] = {
          'P' => sort,
          'R' => []
        }
      elsif command == 's'
        raise ASOSkip
      else
        @aso[artist] = {
          'P' => command,
          'R' => [sort]
        }
      end
    elsif @aso[artist]['P'] != sort && !@aso[artist]['R'].include?(sort) && !sort.nil?
      print 'Found sort order ['.c(36) + sort.c(37) + '] for artist ['.c(36) + artist.c(37) + '], but different sort order ['.c(36) + @aso[artist]['P'].c(37) + '] is already stored. Enter to ignore, ['.c(36) + 'r'.c(37) + '] to replace: '.c(36)
      command = query_stdin
      if command == 'r'
        old_p = @aso[artist]['P']
        @aso[artist]['R'] << old_p
        @aso[artist]['P'] = sort
      elsif command == 's'
        raise ASOSkip
      else
        @aso[artist]['R'] << sort
      end
    end
  end

  def query(artist)
    return nil if artist.nil?

    if @aso[artist].nil?
      query_new(artist)
    else
      @aso[artist]['P']
    end
  end

  def query_new(artist)
    return nil if artist.nil?

    new_sort = query_new_actual(artist)
    @aso[artist] = {
      'P' => new_sort,
      'R' => []
    }
    new_sort
  end

  def query_new_actual(artist)
    print 'Enter sort order for artist ['.c(36) + artist.c(37) + ']: '.c(36)
    result = query_stdin
    raise ASOSkip if result == 's'
    result.empty? ? artist : result
  end

  def query_stdin
    STDIN.gets.chomp
  end
end

aso = ASO.new(config['aso_file'])

mapping_file = config['mapping_file']
mapping = File.exist?(mapping_file) ? JSON.parse(File.read(mapping_file)) : {}

def save_mapping(mapping_file, mapping)
  File.write(mapping_file, JSON.pretty_generate(mapping))
end

def map_artist(config, artist)
  config['artist_remap'].key?(artist) ? config['artist_remap'][artist] : artist
end

def resolve_group(config, short)
  config['group_shorts'].key?(short) ? config['group_shorts'][short] : short
end

def try_load_caa(global_tags)
  unless global_tags[:mb_release_id].nil?
    Thread.new do
      rel_id = global_tags[:mb_release_id]
      begin
        $cover_art_data[rel_id] = URI.open("https://coverartarchive.org/release/#{rel_id}/front")
      rescue
        $cover_art_data[rel_id] = :none
      end
    end
  else
    nil
  end
end

def check_caa(rel_id, thread)
  if thread.nil?
    cl 33, 'No MusicBrainz release ID found, or CAA retrieval is disabled.'
    return nil
  end

  if thread.alive?
    cl 34, 'Retrieval thread is still running. Waiting for retrieval to finish...'
    thread.join
  end

  result = $cover_art_data[rel_id]

  if result.nil?
    cl 31, 'Got nil! Something went wrong.'
    return nil
  end

  if result == :none
    cl 33, 'No cover art was found in the CAA...'
    return nil
  end

  result
end

def read_source(source, aso)
  is_dir = File.directory?(source)

  if is_dir
    audio_files = Dir.glob('**/*.{flac,mp3,m4a,ogg,FLAC,MP3,M4A,OGG,opus,OPUS}', base: source)
    if ENV.key?('RADISH_EXCLUDE')
      regex = Regexp.new(ENV['RADISH_EXCLUDE'])
      rejected, audio_files = audio_files.partition { |e| e.match?(regex) }
      cl 35, 'Rejected ', rejected.length, ' files:'
      rejected.each do |e|
        cl 35, ' ', e
      end
    end
    source_dir = source
  else
    audio_files = [File.basename(source)]
    source_dir = File.dirname(source)
  end

  cl 34, 'Reading tags...'

  all_tags = audio_files.map do |e|
    full_path = File.join(source_dir, e)
    codec_name = ffprobe_stream_property(full_path, 'codec_name')
    value = read_tags(full_path, codec_name)
    value[:codec_name] = codec_name
    [e, value]
  end
  all_tags = Hash[all_tags]

  aso.ingest(all_tags.values)
  [is_dir, audio_files, source_dir, all_tags]
end

TARGET_COMPONENT_MAPPING = [
  [:library_path, 37],
  [:shelf_folder, 94],
  [:aa_path, 95],
  [:new_folder_name, 96],
  [:basename, 97],
]

def finalise_target(target_components, select_components = nil)
  final_target_path_components = []
  target_display = ''

  if select_components.nil?
    tcm = TARGET_COMPONENT_MAPPING
  else
    tcm = TARGET_COMPONENT_MAPPING.select { |k, v| select_components.include?(k) }
  end

  mapped = tcm.map { |k, v| [target_components[k], v] }.reject { |k, v| k.nil? }

  mapped.each do |k, v|
    final_target_path_components << k
    slash = (k == mapped.last.first) ? '' : '/'
    target_display += (k + slash).c(v)
  end

  target = File.join(*final_target_path_components)

  [target, target_display]
end

def mv(mapping_file, mapping, source, target)
  system 'mv', source, target
  mapping[source] = target
  save_mapping(mapping_file, mapping)
  cl 32, 'Moved ', source, ' to ', target
end

if ARGV[0] == 'mv'
  if ARGV.length != 3
    err 'radish mv requires 2 additional arguments!'
  end

  mv(mapping_file, mapping, ARGV[1], ARGV[2])
  exit
elsif ARGV[0] == 'mv_g' # move into different group, one layer up from source
  if ARGV.length != 3
    err 'radish mv_g requires 2 additional arguments!'
  end

  source = ARGV[1]
  source_group = File.dirname(source)
  source_shelf = File.dirname(source_group)
  target = File.join(source_shelf, ARGV[2], File.basename(ARGV[1]))

  mv(mapping_file, mapping, source, target)
  exit
elsif ARGV[0] == 'aso'
  if ARGV.length != 2
    err 'radish aso requires 1 additional argument!'
  end

  new_so = aso.query_new_actual(ARGV[1])
  aso.ingest_one_manual(ARGV[1], new_so)
  aso.save
  exit
elsif ARGV[0] == 'subsort'
  if ARGV.length < 2
    err 'radish subsort requires at least 1 additional argument!'
  end

  ARGV[1..-1].each do |source|
    hr2 80
    cl 34, 'Processing: ', source
    target_dir = pq 'Enter target folder path: '
    until !(target_dir || '').empty? && File.exist?(target_dir)
      print 'Target folder does not yet exist and will be created (or none was specified). Press enter to continue, or enter a different path: '.c(33)
      result = query
      if result.empty?
        FileUtils.mkdir_p target_dir unless (target_dir || '').empty?
        break
      end
      target_dir = result
    end

    abs_source = File.absolute_path(source)
    abs_target = File.absolute_path(File.join(target_dir, File.basename(abs_source)))
    mv(mapping_file, mapping, abs_source, abs_target)
  end

  exit
elsif ARGV[0] == 'find_unmapped'
  all = Dir.glob('**/*.{flac,mp3,ogg,m4a,FLAC,MP3,OGG,M4A,opus,OPUS}').map { |e| File.absolute_path(e) }.sort
  mapped = mapping.keys.sort

  total = 0

  all.each do |e|
    unless mapped.any? { |m| e.start_with?(m) }
      total += 1
      puts "unmapped: #{e}"
    end
  end

  puts "total: #{total}"
  exit
elsif ARGV[0] == 'aso_fix'
  all = Dir.glob('**/*.{flac,mp3,ogg,m4a,FLAC,MP3,OGG,M4A,opus,OPUS}', base: config['library_path']).sort

  all.each do |e|
    # cl 90, 'reading: ' + e
    full_path = File.join(config['library_path'], e)
    codec_name = ffprobe_stream_property(full_path, 'codec_name')
    tags = read_tags(full_path, codec_name)

    new_aso = aso.query(tags[:artist])
    new_aaso = aso.query(tags[:album_artist])

    new_tags = {}

    if new_aso != tags[:artist_sort]
      ck 33, 'ASO  ', 91, tags[:artist_sort], 90, '  =>  ', 92, new_aso, 90, '  in file: ', 94, e
      new_tags[:artist_sort] = new_aso
    end

    if new_aaso != tags[:album_artist_sort]
      ck 33, 'AASO ', 91, tags[:album_artist_sort], 90, '  =>  ', 92, new_aaso, 90, '  in file: ', 94, e
      new_tags[:album_artist_sort] = new_aaso
    end

    mapped_a = map_artist(config, tags[:artist])
    mapped_aa = map_artist(config, tags[:album_artist])

    if tags[:artist] != mapped_a
      ck 33, 'A    ', 91, tags[:artist], 90, '  =>  ', 92, mapped_a, 90, '  in file: ', 94, e
      new_tags[:artist] = mapped_a
    end

    if tags[:album_artist] != mapped_aa
      ck 33, 'AA   ', 91, tags[:album_artist], 90, '  =>  ', 92, mapped_aa, 90, '  in file: ', 94, e
      new_tags[:album_artist] = mapped_aa
    end
  end

  exit
elsif ARGV[0] == 'cover_ext_fix'
  all = Dir.glob('**/cover', base: config['library_path']).sort

  all.each do |e|
    full_path = File.join(config['library_path'], e)

    if File.directory?(full_path)
      cl 33, 'Skipping directory ', full_path
      next
    end

    codec_name = ffprobe_stream_property(full_path, 'codec_name', 'v:0')
    cover_ext = nil

    case codec_name
    when 'mjpeg'
      cover_ext = '.jpg'
    when 'png'
      cover_ext = '.png'
    else
      cl 31, 'Could not determine correct extension for cover file: ', e
      next
    end

    FileUtils.mv full_path, full_path + cover_ext
    ck 92, cover_ext + '  ', 94, e
  end

  exit
elsif ARGV[0] == 'rdb_update'
  if ARGV.length != 2
    err 'radish rdb_update requires 1 additional argument!'
  end

  db_data = File.read(ARGV[1])

  #p̶̠̹͙̲̒͒a̴̩͍̎ṙ̴̥̠͇s̴̡͎͉̽͠i̷̋͌̄͜n̷̫͓̄̐̒͠g̷̢̭̣͈̏ ̴͚̈͠x̴̤̹̱̒̈́̑ͅm̸̻̂̈l̸̻͒͗ ̴͎̠̮̣͒͘w̵̟̻̄̈͘͝i̵̛̗͛͗̋t̸͓͐̓ḥ̴͝ͅ ̸̡̜̎r̵̤̫͌̂̆ȅ̶̩̗̘̙̏̾̋g̵̯̥͑ḝ̴̜͘͝͝x̸̞̮̟͆̓̀
  files = db_data.scan(/file:\/\/(\/[^<]+)</)
  parser = URI::Parser.new

  keys = mapping.keys

  files.each do |e, _|
    fixed = parser.unescape(e.gsub('&amp;', '&'))

    prefixes = keys.select { |e2| fixed.start_with?(e2) }

    if prefixes.empty?
      cl 33, 'Unmapped file: ', fixed
      next
    end
    longest_mapped_prefix = prefixes.max_by(&:length)
    mapped = fixed.sub(longest_mapped_prefix, mapping[longest_mapped_prefix])

    #ck 91, fixed, 90, '  =>  ', 92, mapped

    encoded_orig = parser.escape(fixed).gsub(';', '%3B').gsub('&', '&amp;').gsub('[', '%5B').gsub(']', '%5D')
    if encoded_orig != e
      puts e
      puts encoded_orig
      exit
    end

    encoded = parser.escape(mapped).gsub(';', '%3B').gsub('&', '&amp;').gsub('[', '%5B').gsub(']', '%5D')
    db_data = db_data.gsub(e, encoded)
  end

  File.write('new_db_data.xml', db_data)

  exit
end

fast_forward = false

ARGV.each do |source|
  source = File.absolute_path(source)

  hr2 80
  unless File.exist?(source)
    cl 31, 'File/folder does not exist: ', source
    next
  end

  if mapping.key?(source)
    cl 31, 'File/folder has already been processed: ', source
    next if config['always_skip_mapped'] || fast_forward
    command = pq 'Enter to skip, [y] to process again, [f] to fast-forward: '
    if command == 'f'
      fast_forward = true
      next
    end
    next unless command == 'y'
  end

  cl 34, 'Processing: ', source

  is_dir, audio_files, source_dir, all_tags = read_source(source, aso)

  if audio_files.empty?
    cl 31, 'No audio files found! Skipping source.'
    next
  end

  if config['run_beets']
    input = pq 'Running beets; [n] to cancel, [s] to skip source entirely, [f] to enter CLI flags: '

    next if input == 's'

    if input != 'n'
      beet_cmd = config['beet_command'].clone
      if input == 'f'
        print 'Enter flags: '
        beet_cmd += query.split(' ')
      end

      # When importing only one file
      beet_cmd << '-s' unless is_dir

      beet_cmd << source
      hr 80
      cl 90, (['$'] + beet_cmd).join(' ')
      system *beet_cmd
      hr 80
    end

    # Reread tags
    is_dir, audio_files, source_dir, all_tags = read_source(source, aso)
  else
    cl 35, 'Not running beets'
  end

  cl 32, 'Found ', *cl_s(audio_files.length, ' audio file')

  test_file = File.join(source_dir, audio_files[0])

  bit_depth = ffprobe_stream_property(test_file, 'bits_per_raw_sample')
  sample_rate = ffprobe_stream_property(test_file, 'sample_rate')
  bit_rate = ffprobe_stream_property(test_file, 'bit_rate')
  codec_name = ffprobe_stream_property(test_file, 'codec_name')

  global_tags = all_tags[audio_files[0]]

  caa_thread = nil
  $cover_art_data ||= {}

  caa_thread = try_load_caa(global_tags) if config['load_caa']

  artists = all_tags.values.map { |e| e[:artist] }
  # puts 'All artists: '.c(35) + artists.uniq.sort.map { |e| e.c(95) }.join(', '.c(35))

  ck 90, 'Enter [', 37, '-', 90, '] to leave a field empty.'
  fstr = pq "Audio format [%%]: ", format_str(codec_name, bit_rate, bit_depth, sample_rate)

  album = pq "Album [%%]: ", global_tags[:album]
  album_artist = pq "Album artist [%%]; [v] for various: ", map_artist(config, global_tags[:album_artist])
  album_artist = config['various_artists'] if album_artist == 'v'

  release_date = global_tags[:release_date]
  prompt = "Release date [#{spacey(release_date)}]"
  potential_events = []
  prd = nil
  if date_str?(release_date)
    prd = parse_release_date(release_date)
    potential_events = events_by_date[prd] || []
  end
  prompt += ('; ' + potential_events.map.with_index { |e, i| "[#{i}] for [#{e}]"}.join(', ')) unless potential_events.empty?
  prompt += ': '

  new_release_date, new_year, event = nil, nil, nil
  reload_option = 0

  loop do
    reload_option += 1
    prompt = prompt.sub(': ', '; [r] to reload event list: ') if reload_option == 2
    release_date_query = pq prompt, release_date
    if release_date_query.nil?
      # Do nothing
    elsif release_date_query == 'r'
      dates_by_event, events_by_date = load_event_file(config)
      next
    elsif release_date_query.match?(/^\d$/)
      i = release_date_query.to_i
      event = potential_events[i]
      new_release_date = prd
    elsif date_str?(release_date_query)
      event = nil
      new_release_date = parse_release_date(release_date_query)
    elsif release_date_query.match?(/^\d{4}$/)
      event = nil
      new_release_date = release_date_query
      new_year = release_date_query.to_i
    else
      event = release_date_query
      if dates_by_event.key?(event)
        new_release_date = dates_by_event[event]
      else
        cl 31, 'Could not find event "', event, '"!'
        next
      end
    end

    break
  end

  dstr = event.nil? ? "[#{new_release_date}]" : "[#{new_release_date}] [#{event}]"
  aa_component = album_artist == config['various_artists'] ? '' : "#{album_artist} - "
  new_folder_name = "#{dstr} #{aa_component}#{album} [#{fstr}]"
  new_folder_name_alt = "#{dstr} #{album} [#{fstr}]"
  new_folder_name_aa_always = "#{dstr} #{album_artist} - #{album} [#{fstr}]"

  puts

  target_components = {}

  if config['shelves'].nil? || config['shelves'].empty?
    cl 34, 'No shelves found. Copying to main library path.'
    group_mode = :none
  else
    cl 90, 'Shelves:'
    folder_lookup = {}
    l = config['shelves'].map { |_, shelf| shelf['short'].length }.max
    config['shelves'].each do |_, shelf|
      folder_lookup[shelf['short']] = shelf
      ck 90, ' [', 37, shelf['short'].rjust(l, ' '), 90, "]: #{shelf['folder']}"
    end

    loop do
      selection = pq 'Select shelf: '

      if selection.nil?
        cl 33, 'No shelf selected. Copying to main library path.'
        group_mode = :none
      elsif folder_lookup.key?(selection)
        shelf = folder_lookup[selection]
        target_components[:shelf_folder] = shelf['folder']
        group_mode = shelf['group'].to_sym
      else
        cl 31, 'Invalid shelf!'
        next
      end

      break
    end

    puts

    target_components[:library_path] = config['library_path']
    target_components[:new_folder_name] = deslash(new_folder_name) unless album.nil?
    target_components[:basename] = File.basename(source) unless is_dir

    case group_mode
    when :album_artist
      aa_group_mapped = config['aa_group_remap'].include?(album_artist) ? config['aa_group_remap'][album_artist] : album_artist
      target_components[:aa_path] = deslash(aa_group_mapped) unless album_artist.nil?
    when :query
      target_components[:aa_path] = resolve_group(config, pq('Enter group folder name [%%]: ', target_components[:aa_path]))

      until File.exist?(finalise_target(target_components, [:library_path, :shelf_folder, :aa_path]).first)
        print "Group folder '".c(33), target_components[:aa_path].c(93), "' does not yet exist and will be created. Press enter to continue, or enter a different path: ".c(33)
        result = query
        break if result.empty?
        target_components[:aa_path] = resolve_group(config, result)
      end
    when :none
      # ignore
    else
      err 'Invalid shelf group mode: ', group_mode
    end

    target = nil
    has_basename = false

    loop do
      target, target_display = finalise_target(target_components)

      puts 'Target: '.c(90) + target_display
      puts '        ' + target.c(90)

      cl 33, '(Target already exists! Consider deleting it before proceeding; otherwise there might be problems)' if File.exist?(target)
      cl 33, "Album artist folder is '", config['various_artists'], "'; consider setting a different one." if target_components[:aa_path] == config['various_artists']

      if aa_component.empty?
        command = pq 'Press enter to continue; [a] to set album artist folder, [v] to add album artist to folder name, [d] to disambiguate: '
      else
        command = pq 'Press enter to continue; [a] to set album artist folder, [c] to omit album artist from folder name, [d] to disambiguate: '
      end

      case command
      when 'a'
        target_components[:aa_path] = pq 'Enter new album artist folder: '
      when 'c'
        target_components[:new_folder_name] = new_folder_name_alt
      when 'd'
        result = pq 'Enter album disambiguation tag [%%]: ', global_tags[:disambiguation]
        target_components[:new_folder_name] = "#{dstr} #{aa_component}#{album} [#{result}] [#{fstr}]"
      when 'v'
        target_components[:new_folder_name] = new_folder_name_aa_always
      else
        break
      end

      puts
    end

    puts

    any_corrections = false

    if album.nil?
      ask_unify_albums = false
    else
      ask_unify_albums = true
      all_albums = all_tags.map { |k, v| v[:album] }.uniq
      if all_albums.length > 1
        puts 'Found more than one album in file tags: '.c(33) + all_albums.compact.map { |e| e.c(93) }.join(', '.c(33))
      elsif all_albums.length == 1 && all_albums.first != album
        cl 33, 'Album in file tags [', all_albums.first, '] does not match queried album [', album, ']'
      else
        ask_unify_albums = false
      end
    end

    unify_albums = false
    unify_target = nil
    if ask_unify_albums
      result = pq "Unify albums? Enter to ignore, [y] to unify to [#{album}], or enter unified album tag: "

      unless result.nil?
        unify_albums = true
        unify_target = result == 'y' ? album : result
      end

      puts
    end

    all_tracks = all_tags.map { |k, v| v[:track] }
    if all_tracks.compact.empty?
      command = pq 'No track numbers found! Enter to ignore, or [y] to try to remap from Booth-style filenames: '
      if command == 'y'
        any_match = false
        all_tags.each do |file, tags|
          basename = File.basename(file)
          match = basename.match(/(\d+)-(\d+)-.*\.flac/)
          if match
            any_match = true
            tags[:track] = match[2].to_i
            tags[:disc] = match[1].to_i + 1
            cl 34, 'Found track number ', tags[:track], ' (disc ', tags[:disc], ') from filename ', file
          end
        end
        cl 33, 'Found no matching filenames...' unless any_match
      end

      puts
    end

    all_tracks = all_tags.map { |k, v| v[:track] } # Redo to catch potentially duplicate Booth-styles
    track_groups = all_tracks.compact.group_by { |e| e }.values.sort_by(&:first)
    if track_groups.any? { |e| e.length > 1 }
      puts 'Found duplicate track numbers! All track numbers: '.c(33) + track_groups.flatten.map { |e| e.to_s.c(93) }.join(', '.c(33))

      disc_groups = Hash[all_tags.values.group_by { |e| e[:disc] }.map { |k, v| [k, v.map { |e| e[:track] }] }]
      if cont_range?(disc_groups.keys) && disc_groups.values.all? { |e| cont_range?(e) }
        cl 32, 'Continous remapping appears possible!'
        command = pq 'Remap? [y] (default) or [n]: '
        if command != 'n'
          i = 1
          df, dl = 1, 1
          total_discs, total_tracks = disc_groups.keys.max, disc_groups.values.map(&:length).sum
          disc_groups.sort_by(&:first).each do |disc, v|
            df = i
            v.sort.each do |track|
              # Evil quadratic algorithm
              all_tags.each do |file, tags|
                if tags[:disc] == disc && tags[:track] == track
                  all_tags[file][:track] = i
                  all_tags[file][:total_tracks] = total_tracks
                  all_tags[file][:total_discs] = total_discs
                end
              end

              i += 1
            end

            dl = i - 1
            cl 34, 'Disc ', disc, ': tracks ', df, '-', dl
          end
        end
      else
        cl 33, 'Unable to remap continuously...'
        pq 'Press enter to continue: '
      end

      puts
    end

    album_artist_sort = aso.query(album_artist) unless album_artist.nil?
    album_sort = nil
    unless album.nil?
      album_sort = pq 'Enter album sort order [%%]: ', global_tags[:album_sort] || album
      puts
    end

    cover_source, cover_ext = nil, nil
    if is_dir
      image_files = Dir.glob('**/*.{jpg,png,jpeg,JPG,PNG,JPEG}', base: source)
      definitive_covers = image_files.select do |e|
        bnd = File.basename(e).downcase
        bnd.start_with?('cover') || bnd.start_with?('folder')
      end

      if config['always_wait_for_caa'] && !caa_thread.nil? && caa_thread.alive? && !definitive_covers.any?
        cl 34, 'Waiting for CAA retrieval to finish...'
        caa_thread.join
      end

      caa_data = $cover_art_data[global_tags[:mb_release_id]]

      if definitive_covers.any?
        cover_source = File.join(source, definitive_covers.first)
        cover_ext = File.extname(cover_source)
      elsif !caa_data.nil? && caa_data != :none
        cl 32, 'Using cover art from the CAA'
        cover_source = config['cover_file']
        File.write(cover_source, caa_data.read)
      else
        cl 90, 'Could not find definitive cover image. Image files found:'

        l = (image_files.length - 1).to_s.length
        image_files.each_with_index do |image_file, i|
          size = File.size(File.join(source, image_file))
          cl 90, ' [', i.to_s.rjust(l, ' '), "]: #{image_file} (#{size} bytes)"
        end

        loop do
          command = pq 'Select one of the above, paste a path or URL ([c] to copy metadata), [m] to check the CAA, or press enter for no cover art: '
          if command.nil?
            # Do nothing
          elsif command == 'c'
            str = "#{album_artist} - #{album}"
            system *(config['clip_command'] + [str])
            cl 32, "Copied '", str, "' to clipboard!"
            next
          elsif command == 'm'
            caa_thread = try_load_caa(global_tags) unless config['load_caa'] # if we haven't done this before, do it now
            caa_data = check_caa(global_tags[:mb_release_id], caa_thread)
            next if caa_data.nil?
            cover_source = config['cover_file']
            File.write(cover_source, caa_data.read)
          elsif command.match?(/^\d+$/) && command.to_i < image_files.length
            cover_source = File.join(source, image_files[command.to_i])
          else
            url = command.strip.gsub(/(^')|('$)/, '').gsub("'\\''", "'")

            if url.start_with?('/')
              cover_source = url
            else
              # Try to load remotely
              cover_source = config['cover_file']
              begin

                begin
                  uri = URI.parse(url)
                rescue URI::InvalidURIError
                  uri = URI.parse(URI::Parser.new.escape(url))
                end

                data = URI.open(uri).read
                File.write(cover_source, data)
              rescue => e
                cl 31, 'Error while processing remote image: ', e.to_s
                puts e.backtrace
                next
              end
            end
          end
          break
        end
      end

      if !cover_source.nil? && cover_ext.nil?
        codec_name = ffprobe_stream_property(cover_source, 'codec_name', 'v:0')
        case codec_name
        when 'mjpeg'
          cover_ext = '.jpg'
        when 'png'
          cover_ext = '.png'
        else
          cl 34, 'Converting ', codec_name, ' image to png.'
          safe_run('ffmpeg', '-y', '-i', cover_source, config['cover_converted_file'])
          cover_source = config['cover_converted_file']
          cover_ext = '.png'
        end
      end
    end

    target_container_dir = File.dirname(target)

    FileUtils.mkdir_p target_container_dir

    target_dir = is_dir ? target : File.dirname(target)

    full_copy_command = config['copy_command'] + [source, target]
    puts (['$'] + full_copy_command).join(' ').c(90)
    system *full_copy_command
    mapping[source] = target
    save_mapping(mapping_file, mapping)
    cl 32, 'Copied!'

    max_track = all_tags.values.map { |e| e[:track] }.compact.max
    max_disc = all_tags.values.map { |e| e[:disc] }.compact.max

    extra_album_sorts = {}

    all_tags.each do |file, tags|
      path = File.join(target_dir, file)
      new_tags = tags.clone

      if config['artist_remap'].key?(tags[:artist])
        new_tags[:artist] = config['artist_remap'][tags[:artist]]
      else
        new_tags.delete(:artist)
      end

      new_tags[:album] = unify_target if unify_albums
      new_tags[:album_artist] = album_artist
      new_tags[:album_artist_sort] = album_artist_sort
      new_tags[:artist_sort] = aso.query(new_tags[:artist] || tags[:artist])
      new_tags[:release_date] = new_release_date
      new_tags[:year] = new_year unless new_year.nil?

      if new_tags[:album] == album
        new_tags[:album_sort] = album_sort
      else
        if extra_album_sorts.key?(new_tags[:album])
          new_tags[:album_sort] = extra_album_sorts[new_tags[:album]]
        else
          new_tags[:album_sort] = pq 'Enter additional album sort order [%%]: ', new_tags[:album]
          extra_album_sorts[new_tags[:album]] = new_tags[:album_sort]
        end
      end

      new_tags[:total_tracks] = max_track if !new_tags[:track].nil? && new_tags[:total_tracks].nil?
      new_tags[:total_discs] = max_disc if !new_tags[:disc].nil? && new_tags[:total_discs].nil?

      write_tags(path, tags[:codec_name], new_tags)
    end

    unless cover_source.nil?
      (all_tags.map { |k, v| File.dirname(k) } + ['.']).uniq.each do |tfwmf|
        new_cover_path = File.join(target_dir, tfwmf, 'cover' + cover_ext)
        unless File.exist?(new_cover_path)
          cover_copy_command = config['copy_command'] + [cover_source, new_cover_path]
          system *cover_copy_command
          cl 34, 'Copied cover to ', new_cover_path
        end
      end
    end

    aso.save

    cl 32, 'Done!'
  end
end
