import unittest

from scripts.fetch_daily_dd import build_query, parse_influx_payload, select_new_rows


class FetchDailyDdTests(unittest.TestCase):
    def test_build_query_uses_last_csv_timestamp(self) -> None:
        query = build_query("2026-04-30T00:00:00Z", "Leipzig")

        self.assertIn("time >= '2026-04-30T00:00:00Z'", query)
        self.assertIn('FROM "nodes_communities"', query)
        self.assertIn('"community" = \'Leipzig\'', query)

    def test_parse_influx_payload_sorts_rows_and_skips_nulls(self) -> None:
        payload = {
            "results": [
                {
                    "series": [
                        {
                            "values": [
                                [1746144000, 210],
                                [1746057600, 205.0],
                                [1746230400, None],
                            ]
                        }
                    ]
                }
            ]
        }

        self.assertEqual(
            parse_influx_payload(payload),
            [
                ("2025-05-01T00:00:00Z", 205),
                ("2025-05-02T00:00:00Z", 210),
            ],
        )

    def test_select_new_rows_requires_overlap(self) -> None:
        existing_rows = [("2026-04-30T00:00:00Z", 202)]
        fetched_rows = [("2026-05-01T00:00:00Z", 203)]

        with self.assertRaisesRegex(ValueError, "overlap"):
            select_new_rows(existing_rows, fetched_rows)

    def test_select_new_rows_only_returns_newer_rows(self) -> None:
        existing_rows = [("2026-04-30T00:00:00Z", 202)]
        fetched_rows = [
            ("2026-04-30T00:00:00Z", 205),
            ("2026-05-01T00:00:00Z", 206),
            ("2026-05-02T00:00:00Z", 207),
        ]

        self.assertEqual(
            select_new_rows(existing_rows, fetched_rows),
            [
                ("2026-05-01T00:00:00Z", 206),
                ("2026-05-02T00:00:00Z", 207),
            ],
        )


if __name__ == "__main__":
    unittest.main()