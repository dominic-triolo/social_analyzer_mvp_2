"""Tests for app.services.openai_client — OpenAI GPT-4o vision, Whisper, and analysis helpers."""
import json
import pytest
from unittest.mock import patch, MagicMock, mock_open


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _mock_chat_response(content_dict_or_str):
    """Build a MagicMock that looks like openai ChatCompletion response."""
    if isinstance(content_dict_or_str, dict):
        text = json.dumps(content_dict_or_str)
    else:
        text = content_dict_or_str
    message = MagicMock()
    message.content = text
    choice = MagicMock()
    choice.message = message
    response = MagicMock()
    response.choices = [choice]
    return response


def _mock_transcription(text: str):
    """Build a MagicMock that looks like a Whisper transcription response."""
    transcript = MagicMock()
    transcript.text = text
    return transcript


# ── analyze_content_item — IMAGE path ────────────────────────────────────────

class TestAnalyzeContentItemImage:
    """analyze_content_item() for IMAGE media format."""

    @patch('app.services.openai_client.client')
    def test_returns_image_analysis(self, mock_client):
        from app.services.openai_client import analyze_content_item

        expected = {
            "summary": "A travel photo showing mountains.",
            "niche_theme": "travel",
            "shows_pov": True,
            "shows_authenticity": True,
            "shows_vulnerability": False,
            "facilitates_engagement": True,
            "event_promotion": False,
        }
        mock_client.chat.completions.create.return_value = _mock_chat_response(expected)

        result = analyze_content_item("https://example.com/photo.jpg", "IMAGE")

        assert result["type"] == "IMAGE"
        assert result["url"] == "https://example.com/photo.jpg"
        assert result["summary"] == "A travel photo showing mountains."
        assert result["niche_theme"] == "travel"
        assert result["shows_pov"] is True

    @patch('app.services.openai_client.client')
    def test_image_calls_gpt4o(self, mock_client):
        from app.services.openai_client import analyze_content_item

        mock_client.chat.completions.create.return_value = _mock_chat_response({
            "summary": "x", "niche_theme": "x", "shows_pov": False,
            "shows_authenticity": False, "shows_vulnerability": False,
            "facilitates_engagement": False, "event_promotion": False,
        })

        analyze_content_item("https://example.com/photo.jpg", "IMAGE")

        call_kwargs = mock_client.chat.completions.create.call_args[1]
        assert call_kwargs["model"] == "gpt-4o"
        assert call_kwargs["response_format"] == {"type": "json_object"}

    @patch('app.services.openai_client.client')
    def test_image_passes_url_in_messages(self, mock_client):
        from app.services.openai_client import analyze_content_item

        mock_client.chat.completions.create.return_value = _mock_chat_response({
            "summary": "x", "niche_theme": "x", "shows_pov": False,
            "shows_authenticity": False, "shows_vulnerability": False,
            "facilitates_engagement": False, "event_promotion": False,
        })

        analyze_content_item("https://example.com/img.jpg", "IMAGE")

        messages = mock_client.chat.completions.create.call_args[1]["messages"]
        content_parts = messages[0]["content"]
        image_part = [p for p in content_parts if p.get("type") == "image_url"]
        assert len(image_part) == 1
        assert image_part[0]["image_url"]["url"] == "https://example.com/img.jpg"


# ── analyze_content_item — VIDEO path ────────────────────────────────────────

class TestAnalyzeContentItemVideo:
    """analyze_content_item() for VIDEO media format — transcribes then analyzes."""

    @patch('app.services.openai_client.transcribe_video_with_whisper', return_value="Hello world")
    @patch('app.services.openai_client.client')
    def test_returns_video_analysis(self, mock_client, mock_transcribe):
        from app.services.openai_client import analyze_content_item

        expected = {
            "summary": "Creator discusses travel tips.",
            "niche_theme": "travel",
            "shows_pov": True,
            "shows_authenticity": False,
            "shows_vulnerability": False,
            "facilitates_engagement": True,
            "event_promotion": False,
        }
        mock_client.chat.completions.create.return_value = _mock_chat_response(expected)

        result = analyze_content_item("https://example.com/video.mp4", "VIDEO")

        assert result["type"] == "VIDEO"
        assert result["url"] == "https://example.com/video.mp4"
        assert result["summary"] == "Creator discusses travel tips."

    @patch('app.services.openai_client.transcribe_video_with_whisper', return_value="transcript text")
    @patch('app.services.openai_client.client')
    def test_video_includes_transcript_in_prompt(self, mock_client, mock_transcribe):
        from app.services.openai_client import analyze_content_item

        mock_client.chat.completions.create.return_value = _mock_chat_response({
            "summary": "x", "niche_theme": "x", "shows_pov": False,
            "shows_authenticity": False, "shows_vulnerability": False,
            "facilitates_engagement": False, "event_promotion": False,
        })

        analyze_content_item("https://example.com/v.mp4", "VIDEO")

        prompt = mock_client.chat.completions.create.call_args[1]["messages"][0]["content"]
        assert "transcript text" in prompt


# ── transcribe_video_with_whisper ────────────────────────────────────────────

class TestTranscribeVideoWithWhisper:
    """transcribe_video_with_whisper() — download, transcribe, retry logic."""

    @patch('app.services.openai_client.os.unlink')
    @patch('app.services.openai_client.client')
    @patch('app.services.openai_client.requests.get')
    def test_returns_transcript_text(self, mock_get, mock_client, mock_unlink):
        from app.services.openai_client import transcribe_video_with_whisper

        mock_response = MagicMock()
        mock_response.content = b'\x00\x00\x00\x1cftyp'
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        mock_client.audio.transcriptions.create.return_value = _mock_transcription("Hello world")

        result = transcribe_video_with_whisper("https://example.com/video.mp4")
        assert result == "Hello world"

    @patch('app.services.openai_client.os.unlink')
    @patch('app.services.openai_client.client')
    @patch('app.services.openai_client.requests.get')
    def test_cleans_up_temp_file(self, mock_get, mock_client, mock_unlink):
        from app.services.openai_client import transcribe_video_with_whisper

        mock_response = MagicMock()
        mock_response.content = b'\x00'
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        mock_client.audio.transcriptions.create.return_value = _mock_transcription("ok")

        transcribe_video_with_whisper("https://example.com/v.mp4")
        mock_unlink.assert_called_once()

    @patch('app.services.openai_client.os.unlink')
    @patch('app.services.openai_client.time.sleep')
    @patch('app.services.openai_client.client')
    @patch('app.services.openai_client.requests.get')
    def test_retries_on_rate_limit(self, mock_get, mock_client, mock_sleep, mock_unlink):
        from app.services.openai_client import transcribe_video_with_whisper

        mock_response = MagicMock()
        mock_response.content = b'\x00'
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        mock_client.audio.transcriptions.create.side_effect = [
            Exception("rate_limit exceeded"),
            _mock_transcription("success after retry"),
        ]

        result = transcribe_video_with_whisper("https://example.com/v.mp4", max_retries=2)
        assert result == "success after retry"
        mock_sleep.assert_called_once_with(10)

    @patch('app.services.openai_client.os.unlink')
    @patch('app.services.openai_client.time.sleep')
    @patch('app.services.openai_client.client')
    @patch('app.services.openai_client.requests.get')
    def test_raises_after_max_retries_exhausted(self, mock_get, mock_client, mock_sleep, mock_unlink):
        from app.services.openai_client import transcribe_video_with_whisper

        mock_response = MagicMock()
        mock_response.content = b'\x00'
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        mock_client.audio.transcriptions.create.side_effect = Exception("rate_limit error")

        with pytest.raises(Exception, match="rate_limit"):
            transcribe_video_with_whisper("https://example.com/v.mp4", max_retries=2)

    @patch('app.services.openai_client.client')
    @patch('app.services.openai_client.requests.get')
    def test_raises_immediately_on_non_rate_limit_error(self, mock_get, mock_client):
        from app.services.openai_client import transcribe_video_with_whisper

        mock_response = MagicMock()
        mock_response.content = b'\x00'
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        mock_client.audio.transcriptions.create.side_effect = Exception("invalid_api_key")

        with pytest.raises(Exception, match="invalid_api_key"):
            transcribe_video_with_whisper("https://example.com/v.mp4", max_retries=3)

    @patch('app.services.openai_client.requests.get')
    def test_raises_on_download_failure(self, mock_get):
        from app.services.openai_client import transcribe_video_with_whisper

        mock_get.side_effect = Exception("Connection refused")

        with pytest.raises(Exception, match="Connection refused"):
            transcribe_video_with_whisper("https://example.com/v.mp4", max_retries=1)

    @patch('app.services.openai_client.os.unlink')
    @patch('app.services.openai_client.time.sleep')
    @patch('app.services.openai_client.client')
    @patch('app.services.openai_client.requests.get')
    def test_429_string_triggers_retry(self, mock_get, mock_client, mock_sleep, mock_unlink):
        """The '429' substring in the error message should also trigger retry."""
        from app.services.openai_client import transcribe_video_with_whisper

        mock_response = MagicMock()
        mock_response.content = b'\x00'
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        mock_client.audio.transcriptions.create.side_effect = [
            Exception("Error 429 too many requests"),
            _mock_transcription("ok"),
        ]

        result = transcribe_video_with_whisper("https://example.com/v.mp4", max_retries=2)
        assert result == "ok"
        mock_sleep.assert_called_once()


# ── analyze_bio_evidence ─────────────────────────────────────────────────────

class TestAnalyzeBioEvidence:
    """analyze_bio_evidence() — bio parsing and short-bio early return."""

    @patch('app.services.openai_client.client')
    def test_returns_structured_evidence(self, mock_client):
        from app.services.openai_client import analyze_bio_evidence

        expected = {
            "niche_signals": {"niche_identified": True, "niche_description": "Travel", "confidence": 0.9},
            "in_person_events": {"evidence_found": True, "event_types": ["retreats"], "confidence": 0.8},
            "community_platforms": {"evidence_found": False, "platforms": [], "confidence": 0.1},
            "monetization": {"evidence_found": True, "types": ["coaching"], "confidence": 0.7},
        }
        mock_client.chat.completions.create.return_value = _mock_chat_response(expected)

        result = analyze_bio_evidence("Travel photographer | Hosting retreats worldwide | DM for coaching")

        assert result["niche_signals"]["niche_identified"] is True
        assert result["in_person_events"]["event_types"] == ["retreats"]
        assert result["monetization"]["types"] == ["coaching"]

    def test_short_bio_returns_empty_evidence(self):
        """Bio under 10 chars returns zeroed-out structure without calling OpenAI."""
        from app.services.openai_client import analyze_bio_evidence

        result = analyze_bio_evidence("Hi")

        assert result["niche_signals"]["niche_identified"] is False
        assert result["niche_signals"]["confidence"] == 0.0
        assert result["in_person_events"]["evidence_found"] is False
        assert result["community_platforms"]["evidence_found"] is False
        assert result["monetization"]["evidence_found"] is False

    def test_empty_string_returns_empty_evidence(self):
        from app.services.openai_client import analyze_bio_evidence

        result = analyze_bio_evidence("")

        assert result["niche_signals"]["niche_identified"] is False

    def test_none_bio_returns_empty_evidence(self):
        from app.services.openai_client import analyze_bio_evidence

        result = analyze_bio_evidence(None)

        assert result["niche_signals"]["niche_identified"] is False

    def test_whitespace_only_bio_returns_empty_evidence(self):
        from app.services.openai_client import analyze_bio_evidence

        result = analyze_bio_evidence("         ")

        assert result["niche_signals"]["niche_identified"] is False

    @patch('app.services.openai_client.client')
    def test_bio_exactly_10_chars_calls_openai(self, mock_client):
        """A bio with exactly 10 non-whitespace chars should call the API."""
        from app.services.openai_client import analyze_bio_evidence

        expected = {
            "niche_signals": {"niche_identified": True, "niche_description": "Test", "confidence": 0.5},
            "in_person_events": {"evidence_found": False, "event_types": [], "confidence": 0.0},
            "community_platforms": {"evidence_found": False, "platforms": [], "confidence": 0.0},
            "monetization": {"evidence_found": False, "types": [], "confidence": 0.0},
        }
        mock_client.chat.completions.create.return_value = _mock_chat_response(expected)

        result = analyze_bio_evidence("0123456789")

        mock_client.chat.completions.create.assert_called_once()
        assert result["niche_signals"]["niche_identified"] is True


# ── analyze_caption_evidence ─────────────────────────────────────────────────

class TestAnalyzeCaptionEvidence:
    """analyze_caption_evidence() — caption analysis and empty-list early return."""

    @patch('app.services.openai_client.client')
    def test_returns_structured_evidence(self, mock_client):
        from app.services.openai_client import analyze_caption_evidence

        expected = {
            "in_person_events": {"evidence_found": True, "mention_count": 3, "confidence": 0.8},
            "community_platforms": {"evidence_found": True, "mention_count": 1, "confidence": 0.6},
            "audience_engagement": {"asks_questions": True, "question_count": 4, "confidence": 0.9},
            "authenticity_vulnerability": {
                "shares_personal_details": True, "shows_vulnerability": False,
                "degree": 0.5, "post_count": 2,
            },
        }
        mock_client.chat.completions.create.return_value = _mock_chat_response(expected)

        result = analyze_caption_evidence(["Join our retreat!", "What do you think?"])

        assert result["in_person_events"]["evidence_found"] is True
        assert result["audience_engagement"]["asks_questions"] is True

    def test_empty_captions_returns_zeroed_structure(self):
        from app.services.openai_client import analyze_caption_evidence

        result = analyze_caption_evidence([])

        assert result["in_person_events"]["evidence_found"] is False
        assert result["in_person_events"]["mention_count"] == 0
        assert result["community_platforms"]["evidence_found"] is False
        assert result["audience_engagement"]["asks_questions"] is False
        assert result["authenticity_vulnerability"]["shares_personal_details"] is False

    def test_none_captions_returns_zeroed_structure(self):
        from app.services.openai_client import analyze_caption_evidence

        result = analyze_caption_evidence(None)

        assert result["in_person_events"]["evidence_found"] is False

    @patch('app.services.openai_client.client')
    def test_captions_truncated_to_500_chars(self, mock_client):
        from app.services.openai_client import analyze_caption_evidence

        expected = {
            "in_person_events": {"evidence_found": False, "mention_count": 0, "confidence": 0.0},
            "community_platforms": {"evidence_found": False, "mention_count": 0, "confidence": 0.0},
            "audience_engagement": {"asks_questions": False, "question_count": 0, "confidence": 0.0},
            "authenticity_vulnerability": {
                "shares_personal_details": False, "shows_vulnerability": False,
                "degree": 0.0, "post_count": 0,
            },
        }
        mock_client.chat.completions.create.return_value = _mock_chat_response(expected)

        long_caption = "x" * 1000
        analyze_caption_evidence([long_caption])

        prompt = mock_client.chat.completions.create.call_args[1]["messages"][0]["content"]
        # The caption in the prompt should not contain the full 1000-char string
        assert "x" * 501 not in prompt

    @patch('app.services.openai_client.client')
    def test_empty_strings_in_captions_are_filtered(self, mock_client):
        from app.services.openai_client import analyze_caption_evidence

        expected = {
            "in_person_events": {"evidence_found": False, "mention_count": 0, "confidence": 0.0},
            "community_platforms": {"evidence_found": False, "mention_count": 0, "confidence": 0.0},
            "audience_engagement": {"asks_questions": False, "question_count": 0, "confidence": 0.0},
            "authenticity_vulnerability": {
                "shares_personal_details": False, "shows_vulnerability": False,
                "degree": 0.0, "post_count": 0,
            },
        }
        mock_client.chat.completions.create.return_value = _mock_chat_response(expected)

        analyze_caption_evidence(["", "Real caption", ""])

        prompt = mock_client.chat.completions.create.call_args[1]["messages"][0]["content"]
        assert "Real caption" in prompt
        # Empty captions are filtered by the `if cap` condition in the join
        assert "CAPTION 1" not in prompt


# ── generate_creator_profile ─────────────────────────────────────────────────

class TestGenerateCreatorProfile:
    """generate_creator_profile() — profile synthesis from content analyses."""

    @patch('app.services.openai_client.client')
    def test_returns_profile_with_primary_category(self, mock_client):
        from app.services.openai_client import generate_creator_profile

        expected = {
            "content_category": "Travel and outdoor adventure",
            "primary_category": "Exploration",
            "content_types": ["photos", "reels"],
            "audience_engagement": "Asks questions in captions",
            "creator_presence": "Warm, adventurous personality",
            "monetization": "Sells presets and guides",
            "community_building": "Active Discord server",
        }
        mock_client.chat.completions.create.return_value = _mock_chat_response(expected)

        content = [
            {"type": "IMAGE", "summary": "Mountain landscape photo.", "description": "Epic views"},
            {"type": "VIDEO", "summary": "Hiking vlog.", "description": ""},
        ]
        result = generate_creator_profile(content)

        assert result["primary_category"] == "Exploration"
        assert result["content_category"] == "Travel and outdoor adventure"

    @patch('app.services.openai_client.client')
    def test_adds_unknown_category_when_missing(self, mock_client):
        """If GPT omits primary_category, it defaults to 'unknown'."""
        from app.services.openai_client import generate_creator_profile

        response_without_category = {
            "content_category": "Misc",
            "content_types": [],
            "audience_engagement": "",
            "creator_presence": "",
            "monetization": "",
            "community_building": "",
        }
        mock_client.chat.completions.create.return_value = _mock_chat_response(response_without_category)

        result = generate_creator_profile([{"type": "IMAGE", "summary": "Test"}])

        assert result["primary_category"] == "unknown"

    @patch('app.services.openai_client.client')
    def test_includes_description_in_prompt_when_present(self, mock_client):
        from app.services.openai_client import generate_creator_profile

        mock_client.chat.completions.create.return_value = _mock_chat_response({
            "content_category": "x", "primary_category": "Lifestyle",
            "content_types": [], "audience_engagement": "",
            "creator_presence": "", "monetization": "", "community_building": "",
        })

        content = [{"type": "IMAGE", "summary": "A photo.", "description": "My original caption"}]
        generate_creator_profile(content)

        messages = mock_client.chat.completions.create.call_args[1]["messages"]
        user_msg = messages[1]["content"]
        assert "My original caption" in user_msg

    @patch('app.services.openai_client.client')
    def test_single_content_item(self, mock_client):
        from app.services.openai_client import generate_creator_profile

        mock_client.chat.completions.create.return_value = _mock_chat_response({
            "content_category": "Fitness", "primary_category": "Fitness & sport",
            "content_types": ["reels"], "audience_engagement": "none",
            "creator_presence": "energetic", "monetization": "none",
            "community_building": "none",
        })

        result = generate_creator_profile([{"type": "VIDEO", "summary": "Workout video."}])
        assert result["primary_category"] == "Fitness & sport"


# ── extract_first_names_from_instagram_profile ───────────────────────────────

class TestExtractFirstNames:
    """extract_first_names_from_instagram_profile() — name extraction with fallbacks."""

    @patch('app.services.openai_client.client')
    def test_returns_extracted_name(self, mock_client):
        from app.services.openai_client import extract_first_names_from_instagram_profile

        mock_client.chat.completions.create.return_value = _mock_chat_response("Sarah")

        result = extract_first_names_from_instagram_profile(
            username="sarahtravel", full_name="Sarah Johnson", bio="Travel lover",
        )
        assert result == "Sarah"

    @patch('app.services.openai_client.client')
    def test_strips_quotes_from_response(self, mock_client):
        from app.services.openai_client import extract_first_names_from_instagram_profile

        mock_client.chat.completions.create.return_value = _mock_chat_response('"Sarah"')

        result = extract_first_names_from_instagram_profile(
            username="sarahtravel", full_name="Sarah Johnson", bio="Travel lover",
        )
        assert result == "Sarah"

    @patch('app.services.openai_client.client')
    def test_falls_back_to_full_name_on_empty_response(self, mock_client):
        from app.services.openai_client import extract_first_names_from_instagram_profile

        mock_client.chat.completions.create.return_value = _mock_chat_response("")

        result = extract_first_names_from_instagram_profile(
            username="someone", full_name="Jane Doe", bio="Bio here",
        )
        assert result == "Jane"

    @patch('app.services.openai_client.client')
    def test_falls_back_to_full_name_on_none_response(self, mock_client):
        from app.services.openai_client import extract_first_names_from_instagram_profile

        mock_client.chat.completions.create.return_value = _mock_chat_response("none")

        result = extract_first_names_from_instagram_profile(
            username="someone", full_name="Bob Smith", bio="Bio here",
        )
        assert result == "Bob"

    @patch('app.services.openai_client.client')
    def test_falls_back_to_full_name_on_unknown_response(self, mock_client):
        from app.services.openai_client import extract_first_names_from_instagram_profile

        mock_client.chat.completions.create.return_value = _mock_chat_response("unknown")

        result = extract_first_names_from_instagram_profile(
            username="someone", full_name="Alice Wonder", bio="Bio here",
        )
        assert result == "Alice"

    @patch('app.services.openai_client.client')
    def test_falls_back_to_there_when_no_full_name(self, mock_client):
        from app.services.openai_client import extract_first_names_from_instagram_profile

        mock_client.chat.completions.create.return_value = _mock_chat_response("unknown")

        result = extract_first_names_from_instagram_profile(
            username="someone", full_name="", bio="Bio here",
        )
        assert result == "there"

    @patch('app.services.openai_client.client')
    def test_falls_back_on_api_exception(self, mock_client):
        from app.services.openai_client import extract_first_names_from_instagram_profile

        mock_client.chat.completions.create.side_effect = Exception("API down")

        result = extract_first_names_from_instagram_profile(
            username="creator", full_name="Tina Turner", bio="Music lover",
        )
        assert result == "Tina"

    @patch('app.services.openai_client.client')
    def test_falls_back_to_there_on_exception_with_no_name(self, mock_client):
        from app.services.openai_client import extract_first_names_from_instagram_profile

        mock_client.chat.completions.create.side_effect = Exception("API down")

        result = extract_first_names_from_instagram_profile(
            username="brand", full_name="", bio="",
        )
        assert result == "there"

    def test_returns_there_when_no_username_and_no_full_name(self):
        """No username + no full_name returns 'there' immediately."""
        from app.services.openai_client import extract_first_names_from_instagram_profile

        result = extract_first_names_from_instagram_profile(
            username="", full_name="", bio="Some bio",
        )
        assert result == "there"

    @patch('app.services.openai_client.client', None)
    def test_returns_full_name_fallback_when_client_is_none(self):
        """If openai_client is None (no API key), falls back to full_name split."""
        from app.services.openai_client import extract_first_names_from_instagram_profile

        result = extract_first_names_from_instagram_profile(
            username="traveler", full_name="Maria Garcia", bio="Exploring",
        )
        assert result == "Maria"

    @patch('app.services.openai_client.client', None)
    def test_returns_there_when_client_none_and_no_full_name(self):
        from app.services.openai_client import extract_first_names_from_instagram_profile

        result = extract_first_names_from_instagram_profile(
            username="traveler", full_name="", bio="",
        )
        assert result == "there"

    @patch('app.services.openai_client.client')
    def test_content_analyses_included_in_prompt(self, mock_client):
        from app.services.openai_client import extract_first_names_from_instagram_profile

        mock_client.chat.completions.create.return_value = _mock_chat_response("Dave")

        content = [
            {"summary": "Dave shares his hiking adventures", "caption": ""},
            {"summary": "", "caption": "Another great day on the trail"},
        ]
        extract_first_names_from_instagram_profile(
            username="davehikes", full_name="Dave Hill", bio="Hiker",
            content_analyses=content,
        )

        prompt = mock_client.chat.completions.create.call_args[1]["messages"][1]["content"]
        assert "Dave shares his hiking adventures" in prompt
        assert "Another great day on the trail" in prompt

    @patch('app.services.openai_client.client')
    def test_uses_gpt4o_mini(self, mock_client):
        from app.services.openai_client import extract_first_names_from_instagram_profile

        mock_client.chat.completions.create.return_value = _mock_chat_response("Test")

        extract_first_names_from_instagram_profile(
            username="test", full_name="Test User", bio="bio",
        )

        call_kwargs = mock_client.chat.completions.create.call_args[1]
        assert call_kwargs["model"] == "gpt-4o-mini"
        assert call_kwargs["temperature"] == 0.3
        assert call_kwargs["max_tokens"] == 50


# ── Integration ──────────────────────────────────────────────────────────────

class TestIntegration:
    """Cross-function behavior tests."""

    @patch('app.services.openai_client.client')
    def test_analyze_then_profile_data_shape_compatible(self, mock_client):
        """Output of analyze_content_item is valid input for generate_creator_profile."""
        from app.services.openai_client import analyze_content_item, generate_creator_profile

        image_response = {
            "summary": "A beautiful travel photo of Bali.",
            "niche_theme": "travel",
            "shows_pov": True,
            "shows_authenticity": True,
            "shows_vulnerability": False,
            "facilitates_engagement": True,
            "event_promotion": False,
        }
        profile_response = {
            "content_category": "Travel photography",
            "primary_category": "Exploration",
            "content_types": ["photos"],
            "audience_engagement": "High",
            "creator_presence": "Warm",
            "monetization": "Presets",
            "community_building": "Discord",
        }
        mock_client.chat.completions.create.side_effect = [
            _mock_chat_response(image_response),
            _mock_chat_response(profile_response),
        ]

        content_item = analyze_content_item("https://example.com/bali.jpg", "IMAGE")
        profile = generate_creator_profile([content_item])

        assert profile["primary_category"] == "Exploration"
        assert "summary" in content_item

    @patch('app.services.openai_client.client')
    def test_bio_and_caption_evidence_combined(self, mock_client):
        """Both bio and caption evidence return compatible structures."""
        from app.services.openai_client import analyze_bio_evidence, analyze_caption_evidence

        bio_resp = {
            "niche_signals": {"niche_identified": True, "niche_description": "Yoga", "confidence": 0.9},
            "in_person_events": {"evidence_found": True, "event_types": ["retreats"], "confidence": 0.8},
            "community_platforms": {"evidence_found": False, "platforms": [], "confidence": 0.0},
            "monetization": {"evidence_found": True, "types": ["workshops"], "confidence": 0.7},
        }
        caption_resp = {
            "in_person_events": {"evidence_found": True, "mention_count": 2, "confidence": 0.9},
            "community_platforms": {"evidence_found": True, "mention_count": 1, "confidence": 0.5},
            "audience_engagement": {"asks_questions": True, "question_count": 3, "confidence": 0.8},
            "authenticity_vulnerability": {
                "shares_personal_details": True, "shows_vulnerability": True,
                "degree": 0.7, "post_count": 4,
            },
        }
        mock_client.chat.completions.create.side_effect = [
            _mock_chat_response(bio_resp),
            _mock_chat_response(caption_resp),
        ]

        bio_evidence = analyze_bio_evidence("Certified yoga instructor | Retreat host | Link in bio")
        caption_evidence = analyze_caption_evidence(["Join my retreat!", "What pose should we try next?"])

        # Both should be usable together in downstream scoring
        assert bio_evidence["in_person_events"]["evidence_found"] is True
        assert caption_evidence["in_person_events"]["evidence_found"] is True
        assert "confidence" in bio_evidence["in_person_events"]
        assert "confidence" in caption_evidence["in_person_events"]
