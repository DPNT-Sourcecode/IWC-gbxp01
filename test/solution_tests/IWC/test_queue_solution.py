from __future__ import annotations

from .utils import call_dequeue, call_enqueue, call_size, iso_ts, run_queue, call_age


def test_enqueue_size_dequeue_flow() -> None:
    run_queue(
        [
            call_enqueue("companies_house", 1, iso_ts(delta_minutes=0)).expect(1),
            call_size().expect(1),
            call_dequeue().expect("companies_house", 1),
        ]
    )


def test_rule_of_3() -> None:
    run_queue(
        [
            call_enqueue("companies_house", 1, iso_ts(delta_minutes=0)).expect(1),
            call_enqueue("bank_statements", 2, iso_ts(delta_minutes=0)).expect(2),
            call_enqueue("id_verification", 1, iso_ts(delta_minutes=0)).expect(3),
            call_enqueue("bank_statements", 1, iso_ts(delta_minutes=0)).expect(4),
            call_dequeue().expect("companies_house", 1),
            call_dequeue().expect("id_verification", 1),
            call_dequeue().expect("bank_statements", 1),
            call_dequeue().expect("bank_statements", 2),
        ]
    )


def test_rule_of_3_with_timestamps() -> None:
    run_queue(
        [
            call_enqueue("companies_house", 1, iso_ts(delta_minutes=5)).expect(1),
            call_enqueue("bank_statements", 1, iso_ts(delta_minutes=6)).expect(2),
            call_enqueue("id_verification", 1, iso_ts(delta_minutes=7)).expect(3),
            call_enqueue("companies_house", 2, iso_ts(delta_minutes=0)).expect(4),
            call_enqueue("id_verification", 2, iso_ts(delta_minutes=1)).expect(5),
            call_enqueue("bank_statements", 2, iso_ts(delta_minutes=2)).expect(6),
            call_dequeue().expect("companies_house", 2),
            call_dequeue().expect("id_verification", 2),
            call_dequeue().expect("bank_statements", 2),
            call_dequeue().expect("companies_house", 1),
            call_dequeue().expect("id_verification", 1),
            call_dequeue().expect("bank_statements", 1),
        ]
    )


def test_timestamp_ordering() -> None:
    run_queue(
        [
            call_enqueue("companies_house", 1, iso_ts(delta_minutes=5)).expect(1),
            call_enqueue("id_verification", 2, iso_ts(delta_minutes=0)).expect(2),
            call_dequeue().expect("id_verification", 2),
            call_dequeue().expect("companies_house", 1),
        ]
    )


def test_timestamp_ordering_same_user() -> None:
    run_queue(
        [
            call_enqueue("companies_house", 1, iso_ts(delta_minutes=10)).expect(1),
            call_enqueue("id_verification", 1, iso_ts(delta_minutes=5)).expect(2),
            call_enqueue("bank_statements", 1, iso_ts(delta_minutes=15)).expect(3),
            call_dequeue().expect("id_verification", 1),
            call_dequeue().expect("companies_house", 1),
            call_dequeue().expect("bank_statements", 1),
        ]
    )


def test_dependency_resolution() -> None:
    run_queue(
        [
            call_enqueue("credit_check", 1, iso_ts(delta_minutes=10)).expect(2),
            call_dequeue().expect("companies_house", 1),
            call_dequeue().expect("credit_check", 1),
        ]
    )


def test_dependency_timestamp_ordering() -> None:
    run_queue(
        [
            call_enqueue("credit_check", 1, iso_ts(delta_minutes=10)).expect(2),
            call_enqueue("bank_statements", 2, iso_ts(delta_minutes=5)).expect(3),
            call_dequeue().expect("companies_house", 1),
            call_dequeue().expect("credit_check", 1),
            call_dequeue().expect("bank_statements", 2),
        ]
    )


def test_deduplication_later_timestamp() -> None:
    run_queue(
        [
            call_enqueue("bank_statements", 1, iso_ts(delta_minutes=0)).expect(1),
            call_enqueue("bank_statements", 1, iso_ts(delta_minutes=5)).expect(1),
            call_dequeue().expect("bank_statements", 1),
        ]
    )


def test_deduplication_earlier_timestamp() -> None:
    run_queue(
        [
            call_enqueue("companies_house", 1, iso_ts(delta_minutes=5)).expect(1),
            call_enqueue("id_verification", 1, iso_ts(delta_minutes=6)).expect(2),
            call_enqueue("companies_house", 1, iso_ts(delta_minutes=0)).expect(2),
            call_dequeue().expect("companies_house", 1),
            call_dequeue().expect("id_verification", 1),
        ]
    )


def test_deduplication_with_dependencies() -> None:
    run_queue(
        [
            call_enqueue("companies_house", 1, iso_ts(delta_minutes=5)).expect(1),
            call_enqueue("credit_check", 1, iso_ts(delta_minutes=10)).expect(2),
            call_size().expect(2),
        ]
    )


def test_bank_statements_deprioritised() -> None:
    run_queue(
        [
            call_enqueue("bank_statements", 1, iso_ts(delta_minutes=0)).expect(1),
            call_enqueue("id_verification", 2, iso_ts(delta_minutes=1)).expect(2),
            call_dequeue().expect("id_verification", 2),
            call_dequeue().expect("bank_statements", 1),
        ]
    )


def test_bank_statements_deprioritised_with_rule_of_3() -> None:
    run_queue(
        [
            call_enqueue("bank_statements", 1, iso_ts(delta_minutes=0)).expect(1),
            call_enqueue("companies_house", 1, iso_ts(delta_minutes=1)).expect(2),
            call_enqueue("id_verification", 1, iso_ts(delta_minutes=2)).expect(3),
            call_dequeue().expect("companies_house", 1),
            call_dequeue().expect("id_verification", 1),
            call_dequeue().expect("bank_statements", 1),
        ]
    )


def test_age_empty_queue() -> None:
    run_queue([call_age().expect(0)])


def test_age_single_task() -> None:
    run_queue(
        [
            call_enqueue("bank_statements", 1, iso_ts(delta_minutes=5)).expect(1),
            call_age().expect(0),
        ]
    )


def test_age_multiple_tasks() -> None:
    run_queue(
        [
            call_enqueue("bank_statements", 1, iso_ts(delta_minutes=0)).expect(1),
            call_enqueue("id_verification", 1, iso_ts(delta_minutes=15)).expect(2),
            call_age().expect(900),
        ]
    )


def test_age_after_dequeue() -> None:
    run_queue(
        [
            call_enqueue("companies_house", 1, iso_ts(delta_minutes=0)).expect(1),
            call_enqueue("id_verification", 1, iso_ts(delta_minutes=15)).expect(2),
            call_age().expect(900),
            call_dequeue().expect("companies_house", 1),
            call_age().expect(0),
        ]
    )

