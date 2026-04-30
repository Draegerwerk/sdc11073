"""Tests localization."""

from __future__ import annotations

import logging
import typing

from sdc11073.xml_types import actions, msg_qnames, msg_types

if typing.TYPE_CHECKING:
    from collections.abc import Iterable, Sequence

    from sdc11073.consumer.serviceclients.localizationservice import LocalizationServiceClient

__STEP__ = '7'
logger = logging.getLogger('pat.consumer')

_EXPECTED_LANGUAGES = ('en-US', 'de', 'el-GR', 'zh-CN')


def get_supported_languages(step: str, localization_service: LocalizationServiceClient) -> Sequence[str] | None:
    """Get supported languages."""
    try:
        result = localization_service.get_supported_languages()
    except Exception:
        logger.exception('Error during GetSupportedLanguages', extra={'step': step})
        return None

    if result.action != actions.Actions.GetSupportedLanguagesResponse:
        logger.error(
            'The reference provider answered to GetSupportedLanguages with an unexpected action: %s.',
            result.action,
            extra={'step': step},
        )
        return None

    if result.msg_qname != msg_qnames.GetSupportedLanguagesResponse:
        logger.error(
            'The reference provider answered to GetSupportedLanguages with an unexpected message element: %s.',
            result.msg_qname,
            extra={'step': step},
        )
        return None

    languages = typing.cast('msg_types.GetSupportedLanguagesResponse', result.result).Lang
    if not languages:
        logger.error(
            'The reference provider answered to GetSupportedLanguages with no %s elements.',
            msg_qnames.Lang,
            extra={'step': step},
        )
        return None
    return languages


def get_localized_texts(
    step: str,
    localization_service: LocalizationServiceClient,
    langs: Iterable[str] | None = None,
    version: int | None = None,
) -> Sequence[msg_types.LocalizedText] | None:
    """Get localized texts."""
    try:
        result = localization_service.get_localized_texts(
            langs=list(langs) if langs is not None else langs, version=version
        )
    except Exception:
        logger.exception('Error during GetLocalizedTexts', extra={'step': step})
        return None

    if result.action != actions.Actions.GetLocalizedTextResponse:
        logger.error(
            'The reference provider answered to GetLocalizedTexts with an unexpected action: %s.',
            result.action,
            extra={'step': step},
        )
        return None

    if result.msg_qname != msg_qnames.GetLocalizedTextResponse:
        logger.error(
            'The reference provider answered to GetLocalizedTexts with an unexpected message element: %s.',
            result.msg_qname,
            extra={'step': step},
        )
        return None

    texts = typing.cast('msg_types.GetLocalizedTextResponse', result.result).Text
    if not texts:
        logger.error(
            'The reference provider answered to GetLocalizedTexts with no %s elements.',
            msg_qnames.Text,
            extra={'step': step},
        )
        return None
    return texts


def test_7a(localization_service: LocalizationServiceClient) -> bool:
    """The Reference Consumer requests GetSupportedLanguages.

    The Reference Provider answers with GetSupportedLanguagesResponse containing all languages currently
    provided by the Localized Text Database (V2: en-US, de, el-GR, zh-CN).
    """
    step = f'{__STEP__}a'

    if (supported_languages := get_supported_languages(step, localization_service)) is None:
        return False

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

    if (supported_languages := get_supported_languages(step, localization_service)) is None:
        return False

    if not supported_languages:
        logger.error('No supported languages returned.', extra={'step': step})
        return False

    test_results: list[bool] = []
    for lang in supported_languages:
        if (texts := get_localized_texts(step, localization_service, langs=[lang])) is None:
            test_results.append(False)
            continue

        unexpected_languages = {text.Lang for text in texts if text.Lang != lang}
        if unexpected_languages:
            logger.error(
                'GetLocalizedText for language %s returned unexpected language(s) %s.',
                lang,
                unexpected_languages,
                extra={'step': step},
            )
            test_results.append(False)
            continue

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
    version = 1

    if (supported_languages := get_supported_languages(step, localization_service)) is None:
        return False

    if not supported_languages:
        logger.error('No supported languages returned.', extra={'step': step})
        return False

    test_results: list[bool] = []
    for lang in supported_languages:
        if (texts := get_localized_texts(step, localization_service, langs=[lang], version=version)) is None:
            test_results.append(False)
            continue

        unexpected_languages = {text.Lang for text in texts if text.Lang != lang}
        if unexpected_languages:
            logger.error(
                'GetLocalizedText for language %s returned unexpected language(s) %s.',
                lang,
                unexpected_languages,
                extra={'step': step},
            )
            test_results.append(False)
            continue

        unexpected_versions = {text.Version for text in texts if text.Version != version}
        if unexpected_versions:
            logger.error(
                'GetLocalizedText for language %s returned unexpected version(s) %s.',
                lang,
                unexpected_versions,
                extra={'step': step},
            )
            test_results.append(False)
            continue

        logger.info(
            'GetLocalizedText for language %s, version %d returned %d text(s).',
            lang,
            version,
            len(texts),
            extra={'step': step},
        )
        test_results.append(True)

    if not test_results:
        logger.error('No GetLocalizedText results collected.', extra={'step': step})
        return False
    return all(test_results)
