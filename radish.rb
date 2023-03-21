require 'taglib'
require 'fileutils'
require 'tomlrb'
require 'date'
require 'json'
require 'open-uri'

# Defines cl, hr2, etc
load '~/Projects/utils.rb'

# radish should manage:
# - album artist
# - track numbers
# - album & artist & AA sort order
# - cover art in folder
# - release date & doujin events
# - folder & file names:
#     [date/event] album artist - album [format]
# - rhythmdb integration

config = Tomlrb.load_file('/home/miras/Projects/radishes/radish.toml')

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
  safe_run('ffprobe', '-v', 'error', '-select_streams', stream_descriptor, '-show_entries', 'stream=' + property, '-of', 'default=noprint_wrappers=1:nokey=1', file, no_stderr: true).strip
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
  raise 'total_tracks must be specified' if tags.key?(:track) && !tags.key?(:total_tracks)
  raise 'total_discs must be specified' if tags.key?(:disc) && !tags.key?(:total_discs)
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

    tags_set.each do |tags|
      ingest_one_manual(tags[:artist], tags[:artist_sort])
      ingest_one_manual(tags[:album_artist], tags[:album_artist_sort])
    end
  end

  def ingest_one_manual(artist, sort)
    return if artist.nil?

    if @aso[artist].nil? && (sort.nil? || artist == sort)
      query_new(artist)
    elsif @aso[artist].nil? && artist != sort
      ck 36, 'Storing sort order [', 37, sort, 36, '] for artist [', 37, artist, 36, ']'
      @aso[artist] = {
        'P' => sort,
        'R' => []
      }
    elsif @aso[artist]['P'] != sort && !@aso[artist]['R'].include?(sort) && !sort.nil?
      print 'Found sort order ['.c(36) + sort.c(37) + '] for artist ['.c(36) + artist.c(37) + '], but different sort order ['.c(36) + @aso[artist]['P'].c(37) + '] is already stored. Enter to ignore, ['.c(36) + 'r'.c(37) + '] to replace: '
      command = query_stdin
      if command == 'r'
        old_p = @aso[artist]['P']
        @aso[artist]['R'] << old_p
        @aso[artist]['P'] = sort
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

    print 'Enter sort order for artist ['.c(36) + artist.c(37) + ']: '.c(36)
    result = query_stdin
    new_sort = result.empty? ? artist : result
    @aso[artist] = {
      'P' => new_sort,
      'R' => []
    }
    new_sort
  end

  def query_stdin
    STDIN.gets.chomp
  end
end

aso = ASO.new(config['aso_file'])

mapping_file = config['mapping_file']
mapping = File.exist?(mapping_file) ? JSON.parse(File.read(mapping_file)) : {}

def read_source(source, aso)
  is_dir = File.directory?(source)

  if is_dir
    audio_files = Dir.glob('**/*.{flac,mp3,m4a,ogg}', base: source)
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

ARGV.each do |source|
  source = File.absolute_path(source)

  hr2 80
  unless File.exist?(source)
    cl 31, 'File/folder does not exist: ', source
    next
  end

  if mapping.key?(source)
    cl 31, 'File/folder has already been processed: ', source
    next if config['always_skip_mapped']
    command = pq 'Enter to skip, [y] to process again: '
    next unless command == 'y'
  end

  cl 34, 'Processing: ', source

  is_dir, audio_files, source_dir, all_tags = read_source(source, aso)

  if config['run_beets']
    input = pq 'Running beets; [n] to skip, [f] to enter CLI flags: '
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

  ck 90, 'Enter [', 37, '-', 90, '] to leave a field empty.'
  fstr = pq "Audio format [%%]: ", format_str(codec_name, bit_rate, bit_depth, sample_rate)

  album = pq "Album [%%]: ", global_tags[:album]
  album_artist = pq "Album artist [%%]; [v] for various: ", global_tags[:album_artist]
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

  puts

  target_components = {}

  if config['shelves'].nil? || config['shelves'].empty?
    cl 34, 'No shelves found. Copying to main library path.'
    aa_group = false
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
        cl 34, 'No shelf selected. Copying to main library path.'
        aa_group = false
      elsif folder_lookup.key?(selection)
        shelf = folder_lookup[selection]
        target_components[:shelf_folder] = shelf['folder']
        aa_group = shelf['aa_group']
      else
        cl 31, 'Invalid shelf!'
        next
      end

      break
    end

    puts

    target_components[:library_path] = config['library_path']
    target_components[:aa_path] = deslash(album_artist) if aa_group && !album_artist.nil?
    target_components[:new_folder_name] = deslash(new_folder_name) unless album.nil?
    target_components[:basename] = File.basename(source) unless is_dir

    target = nil
    has_basename = false

    loop do
      final_target_path_components = []
      target_display = ''

      mapped = TARGET_COMPONENT_MAPPING.map { |k, v| [target_components[k], v] }.reject { |k, v| k.nil? }

      mapped.each do |k, v|
        final_target_path_components << k
        slash = (k == mapped.last.first) ? '' : '/'
        target_display += (k + slash).c(v)
      end

      target = File.join(*final_target_path_components)
      puts 'Target: '.c(90) + target_display
      puts '        ' + target.c(90)

      cl 33, '(Target already exists! Consider deleting it before proceeding; otherwise there might be problems)' if File.exist?(target)
      cl 33, "Album artist folder is '", config['various_artists'], "'; consider setting a different one." if target_components[:aa_path] == config['various_artists']
      command = pq 'Press enter to continue; [a] to set album artist folder: '

      case command
      when 'a'
        target_components[:aa_path] = pq 'Enter new album artist folder: '
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
        puts 'Found more than one album in file tags: '.c(33) + all_albums.map { |e| e.c(93) }.join(', '.c(33))
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
          match = basename.match(/\d+-(\d+)-.*\.flac/)
          if match
            any_match = true
            tags[:track] = match[1].to_i
            cl 34, 'Found track number ', tags[:track], ' from filename ', file
          end
        end
        unless any_match?
          cl 33, 'Found no matching filenames...'
        end
      end

      puts
    end

    track_groups = all_tracks.group_by { |e| e }.values.sort_by(&:first)
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

      if definitive_covers.any?
        cover_source = File.join(source, definitive_covers.first)
      else
        cl 90, 'Could not find definitive cover image. Image files found:'

        l = (image_files.length - 1).to_s.length
        image_files.each_with_index do |image_file, i|
          size = File.size(File.join(source, image_file))
          cl 90, ' [', i.to_s.rjust(l, ' '), "]: #{image_file} (#{size} bytes)"
        end

        loop do
          command = pq 'Select one of the above, paste a URL, or press enter for no cover art: '
          if command.nil?
            # Do nothing
          elsif command.match?(/^\d+$/)
            cover_source = File.join(source, image_files[command.to_i])
          else
            cover_source = config['cover_file']
            begin
              data = URI.open(command.strip).read
              File.write(cover_source, data)
            rescue => e
              cl 31, 'Error while processing remote image: ', e.to_s
              puts e.backtrace
              next
            end
          end
          break
        end

        unless cover_source.nil?
          codec_name = ffprobe_stream_property(cover_source, 'codec_name', 'v:0')
          case codec_name
          when 'mjpeg'
            cover_ext = '.jpg'
          when 'png'
            cover_ext = '.png'
          else
            cl 34, 'Converting ', codec_name, ' image to png.'
            safe_run('ffmpeg', '-i', cover_source, config['cover_converted_file'])
            cover_source = config['cover_converted_file']
            cover_ext = '.png'
          end
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
    File.write(mapping_file, JSON.pretty_generate(mapping))
    cl 32, 'Copied!'

    max_track = all_tags.values.map { |e| e[:track] }.compact.max
    max_disc = all_tags.values.map { |e| e[:disc] }.compact.max

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
      new_tags[:album_sort] = album_sort
      new_tags[:album_artist_sort] = album_artist_sort
      new_tags[:artist_sort] = aso.query(tags[:artist])
      new_tags[:release_date] = new_release_date
      new_tags[:year] = new_year unless new_year.nil?

      new_tags[:total_tracks] = max_track if !new_tags[:track].nil? && new_tags[:total_tracks].nil?
      new_tags[:total_discs] = max_disc if !new_tags[:disc].nil? && new_tags[:total_discs].nil?

      write_tags(path, tags[:codec_name], new_tags)
    end

    unless cover_source.nil?
      cover_ext = File.extname(cover_source) if cover_ext.nil?
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
