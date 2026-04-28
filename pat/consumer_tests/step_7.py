"""Tests localization."""

from __future__ import annotations

import logging
import typing

from sdc11073.xml_types import actions

if typing.TYPE_CHECKING:
    from sdc11073.consumer.serviceclients.localizationservice import LocalizationServiceClient

__STEP__ = '7'
logger = logging.getLogger('pat.consumer')

_EXPECTED_LANGUAGES = ('en-US', 'de', 'el-GR', 'zh-CN')


def test_7a(localization_service: LocalizationServiceClient) -> bool:
    """The Reference Consumer requests GetSupportedLanguages.

    The Reference Provider answers with GetSupportedLanguagesResponse containing all languages currently
    provided by the Localized Text Database (V2: en-US, de, el-GR, zh-CN).
    """
    step = f'{__STEP__}a'

    try:
        result = localization_service.get_supported_languages()
    except Exception:
        logger.exception('Error during GetSupportedLanguages', extra={'step': step})
        return False

    if result.action != actions.Actions.GetSupportedLanguagesResponse:
        logger.error(
            'The reference provider answered to GetSupportedLanguages with an unexpected action: %s.',
            result.action,
            extra={'step': step},
        )
        return False

    supported_languages = result.result.Lang
    logger.info('GetSupportedLanguages returned languages: %s', supported_languages, extra={'step': step})

    missing_languages = [lang for lang in _EXPECTED_LANGUAGES if lang not in supported_languages]
    if missing_languages:
        logger.error(
            'The reference provider is missing expected languages: %s (got %s).',
            missing_languages,
            supported_languages,
            extra={'step': step},
        )
        return False

    logger.info(
        'The reference provider supports all expected languages: %s.',
        _EXPECTED_LANGUAGES,
        extra={'step': step},
    )
    return True


def test_7b(localization_service: LocalizationServiceClient) -> bool:
    """For each language, the Reference Consumer requests GetLocalizedText containing the language.

    The Reference Provider answers with GetLocalizedTextResponse containing all texts of the given language
    and version. The Reference Consumer verifies that the result set is not empty.
    Note: As the Reference Provider is allowed to change localized texts, the actual result set is unknown
    and cannot be verified against the Localized Text Database.
    """
    step = f'{__STEP__}b'

    # First get the supported languages
    try:
        lang_result = localization_service.get_supported_languages()
    except Exception:
        logger.exception('Error during GetSupportedLanguages', extra={'step': step})
        return False

    supported_languages = lang_result.result.Lang
    if not supported_languages:
        logger.error('No supported languages returned.', extra={'step': step})
        return False

    test_results: list[bool] = []
    for lang in supported_languages:
        try:
            result = localization_service.get_localized_texts(langs=[lang])
        except Exception:
            logger.exception('Error during GetLocalizedText for language %s', lang, extra={'step': step})
            test_results.append(False)
            continue

        if result.action != actions.Actions.GetLocalizedTextResponse:
            logger.error(
                'Unexpected action for GetLocalizedText (lang=%s): %s.',
                lang,
                result.action,
                extra={'step': step},
            )
            test_results.append(False)
            continue

        texts = result.result.Text
        if not texts:
            logger.error(
                'GetLocalizedText for language %s returned an empty result set.',
                lang,
                extra={'step': step},
            )
            test_results.append(False)
        else:
            logger.info(
                'GetLocalizedText for language %s returned %d text(s).',
                lang,
                len(texts),
                extra={'step': step},
            )
            test_results.append(True)

    if not test_results:
        logger.error('No GetLocalizedText results collected.', extra={'step': step})
        return False
    return all(test_results)


def test_7c(localization_service: LocalizationServiceClient) -> bool:
    """For each language, the Reference Consumer requests GetLocalizedText with version 1.

    The Reference Provider answers with GetLocalizedTextResponse containing at least all texts from the
    Localized Text Database corresponding to the requested language and version.
    Note: Requesting the latest version can result in a different answer as the Reference Provider may
    change texts at any time.
    """
    step = f'{__STEP__}c'

    # First get the supported languages
    try:
        lang_result = localization_service.get_supported_languages()
    except Exception:
        logger.exception('Error during GetSupportedLanguages', extra={'step': step})
        return False

    supported_languages = lang_result.result.Lang
    if not supported_languages:
        logger.error('No supported languages returned.', extra={'step': step})
        return False

    test_results: list[bool] = []
    for lang in supported_languages:
        try:
            result = localization_service.get_localized_texts(langs=[lang], version=1)
        except Exception:
            logger.exception(
                'Error during GetLocalizedText for language %s, version 1',
                lang,
                extra={'step': step},
            )
            test_results.append(False)
            continue

        if result.action != actions.Actions.GetLocalizedTextResponse:
            logger.error(
                'Unexpected action for GetLocalizedText (lang=%s, version=1): %s.',
                lang,
                result.action,
                extra={'step': step},
            )
            test_results.append(False)
            continue

        texts = result.result.Text
        if not texts:
            logger.error(
                'GetLocalizedText for language %s, version 1 returned an empty result set.',
                lang,
                extra={'step': step},
            )
            test_results.append(False)
        else:
            logger.info(
                'GetLocalizedText for language %s, version 1 returned %d text(s).',
                lang,
                len(texts),
                extra={'step': step},
            )
            test_results.append(True)

    if not test_results:
        logger.error('No GetLocalizedText results collected.', extra={'step': step})
        return False
    return all(test_results)
