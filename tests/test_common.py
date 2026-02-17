#!/usr/bin/env python3
"""
test_common.py - Comprehensive tests for common.py logic
"""

import unittest
import sys
import os
from textwrap import dedent

# Add src directory to path
sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src"))

from common import (
    simhash64,
    normalize_diff,
    hamming_distance,
    compute_simhash_similarity,
    ProvenanceConfig,
    normalize_identifier,
    deep_compare_diffs,
    detect_code_movement,
    is_infrastructure_file,
    filter_branding_changes,
)


class TestCommonCore(unittest.TestCase):
    def test_simhash64_basic(self):
        """Test basic properties of SimHash64 algorithm."""
        text1 = "int main() { return 0; }"
        h1 = simhash64(text1)
        self.assertEqual(h1, simhash64(text1))
        self.assertIsInstance(h1, int)

    def test_hamming_distance(self):
        """Test bitwise Hamming distance calculation."""
        self.assertEqual(hamming_distance(7, 0), 3)
        self.assertEqual(hamming_distance(0xFFFFFFFFFFFFFFFF, 0), 64)

    def test_compute_simhash_similarity(self):
        """Test conversion of Hamming distance to 0.0-1.0 similarity score."""
        self.assertEqual(compute_simhash_similarity(12345, 12345), 1.0)
        self.assertEqual(compute_simhash_similarity(0, 0xFFFFFFFFFFFFFFFF), 0.0)


class TestNormalization(unittest.TestCase):
    def setUp(self):
        self.config = ProvenanceConfig(
            source_brand="Redis", target_brand="Valkey",
            source_prefix="RM_", target_prefix="VM_",
            infrastructure_patterns=[".github/", "deps/", "README", "Makefile"]
        )

    def test_branding_normalization(self):
        """Verify that source and target branding terms normalize to identical tokens."""
        r_diff = "+int *RM_GetCommandKeys(RedisModuleCtx *ctx) { return NULL; }"
        v_diff = "+int *VM_GetCommandKeys(ValkeyModuleCtx *ctx) { return NULL; }"
        self.assertEqual(normalize_diff(r_diff, self.config), normalize_diff(v_diff, self.config))

    def test_identifier_substring_branding(self):
        """Test normalization of identifiers containing branding as substrings."""
        self.assertEqual(normalize_identifier("redis_connection_redis", self.config), "connection_redis")
        self.assertEqual(normalize_identifier("createRedisContext", self.config), "createContext")

    def test_normalization_edge_cases(self):
        """Test normalization of C macros and nested branding terms."""
        diff = "+#define REDIS_MAX 1024\n+int redis_val = REDIS_MAX;"
        norm = normalize_diff(diff, self.config)
        self.assertIn("define MAX NUM", norm)
        self.assertNotIn("redis", norm.lower())

    def test_tcl_normalization(self):
        """Verify that Tcl syntax is correctly tokenized and normalized."""
        diff = "+test \"redis\" { set r [redis_client] }"
        norm = normalize_diff(diff, self.config)
        self.assertIn("test STR { set r [ client ] }", norm)

    def test_large_refactor_block(self):
        """Test similarity score stability across realistic code refactors."""
        r = "+void redisProc(client *c) { if (c->flags & REDIS_WRITE) return; }"
        v = "+void valkeyProc(client *c) { if (c->flags & VALKEY_WRITE) return; }"
        self.assertGreater(compute_simhash_similarity(simhash64(normalize_diff(r, self.config)), simhash64(normalize_diff(v, self.config))), 0.80)

    def test_evasion_exhaustive_comments(self):
        """Ensure all comment types (C, C++, Python) are stripped during normalization."""
        code_a = "+int x = 10; x += 5; return x;"
        code_b = "+int x = 10; /* sneaky */ x += 5; // more \n return x; # python"
        n_a, n_b = normalize_diff(code_a, self.config), normalize_diff(code_b, self.config)
        self.assertEqual(n_a.replace("\n", " "), n_b.replace("\n", " "))

    def test_systematic_type_substitution(self):
        """Verify structural similarity is maintained across type substitutions."""
        d_i = "+int run(int a) { return a * 2; }"
        d_l = "+long run(long a) { return a * 2; }"
        self.assertGreater(compute_simhash_similarity(simhash64(normalize_diff(d_i, self.config)), simhash64(normalize_diff(d_l, self.config))), 0.70)

    def test_multiline_macro_normalization(self):
        """Test normalization of multiline C preprocessor macros."""
        diff = "+#define M(x) \\\n+  do { redis_call(); } while(0)"
        norm = normalize_diff(diff, self.config)
        self.assertIn("define M ( x )", norm)
        self.assertIn("do { call (); } while ( NUM )", norm)

    def test_pointer_arithmetic_normalization(self):
        """Verify complex pointer and operator tokenization."""
        diff = "+int val = *(ptr++) + base->offset[idx];"
        norm = normalize_diff(diff, self.config)
        self.assertIn("int val = *( ptr ++) + base -> offset [ idx ];", norm)

    def test_infrastructure_file_detection(self):
        """Test filtering of non-code infrastructure files."""
        self.assertTrue(is_infrastructure_file("deps/lua/src/lapi.c", self.config))
        self.assertFalse(is_infrastructure_file("src/server.c", self.config))

    def test_code_movement_exact_relocation(self):
        """Detect when a diff consists of exact line relocations (trivial)."""
        diff = "-void f() { x=1; }\n+void f() { x=1; }"
        is_t, _, _, _ = detect_code_movement(diff)
        self.assertTrue(is_t)

    def test_code_movement_partial_modification(self):
        """Verify that significant new content overrides code movement triviality."""
        diff = "-void f() { x=1; }\n+void f() { x=1; }\n+void g() { y=2; }\n+void h() { z=3; }\n+void i() { a=4; }\n+void j() { b=5; }\n+void k() { c=6; }"
        is_t, _, _, _ = detect_code_movement(diff)
        self.assertFalse(is_t)

    def test_config_with_special_characters(self):
        """Ensure branding terms with regex meta-characters are handled safely."""
        cfg = ProvenanceConfig(source_brand="C++")
        diff = "+int C++_Val = 1;"
        norm = normalize_diff(diff, cfg)
        self.assertIn("int C ++ _Val = NUM ;", norm)

    def test_normalization_of_very_small_diff(self):
        """Verify context inclusion heuristic for very small diffs."""
        diff = " void ctx() {}\n+void chg() {}\n void m_ctx() {}"
        norm = normalize_diff(diff, self.config)
        self.assertIn("void ctx () {}", norm)
        self.assertIn("void chg () {}", norm)

    def test_normalization_large_whitespace_blocks(self):
        """Test resilience to irregular whitespace and indentation."""
        self.assertEqual(normalize_diff("+  int  x  =  5 ; ", self.config), "int x = NUM ;")

    def test_filter_branding_only_changes(self):
        """Branding-only replacements should be removed from diff content."""
        diff = "\n".join(
            [
                "diff --git a/src/a.c b/src/a.c",
                "--- a/src/a.c",
                "+++ b/src/a.c",
                "@@ -1 +1 @@",
                "-RedisModuleCtx *ctx = NULL;",
                "+ValkeyModuleCtx *ctx = NULL;",
            ]
        )
        filtered = filter_branding_changes(diff, self.config)
        self.assertNotIn("RedisModuleCtx", filtered)
        self.assertNotIn("ValkeyModuleCtx", filtered)


class TestDeepComparison(unittest.TestCase):
    def setUp(self):
        self.config = ProvenanceConfig(source_brand="Redis", target_brand="Valkey")

    def test_subset_ratio_logic(self):
        """Verify that partial copies (cherry-picks) are correctly detected via subset ratio."""
        v = "+void f() { int x=1; server_f(); }"
        r = "+void g() { int y=2; }\n+void f() { int x=1; redis_f(); }"
        sim, _, _ = deep_compare_diffs(v, r, self.config)
        self.assertGreaterEqual(sim, 0.90)

    def test_extreme_dilution(self):
        """Ensure small matches are not lost within massive unrelated changes."""
        n = "+for (int i=0; i<10; i++) { sum += i; }"
        h = n + "\n" + "\n".join([f"+int v{i}={i};" for i in range(100)])
        sim, _, _ = deep_compare_diffs(n, h, self.config)
        self.assertGreaterEqual(sim, 0.95)



    def test_multi_branding_normalization(self):
        """Verify that multiple branding pairs are normalized correctly."""
        cfg = ProvenanceConfig(
            branding_pairs=[("Redis", "Valkey"), ("KeyDB", "Valkey")]
        )
        self.assertEqual(normalize_identifier("RedisLog", cfg), "Log")
        self.assertEqual(normalize_identifier("KeyDBLog", cfg), "Log")
        self.assertEqual(normalize_identifier("ValkeyLog", cfg), "Log")

    def test_multi_prefix_normalization(self):
        """Verify that multiple prefix pairs are normalized correctly."""
        cfg = ProvenanceConfig(
            prefix_pairs=[("RM_", "VM_"), ("REDISMODULE_", "VALKEYMODULE_")]
        )
        self.assertEqual(normalize_identifier("RM_Call", cfg), "M_Call")
        self.assertEqual(normalize_identifier("VM_Call", cfg), "M_Call")
        self.assertEqual(normalize_identifier("REDISMODULE_OK", cfg), "M_OK")
        self.assertEqual(normalize_identifier("VALKEYMODULE_OK", cfg), "M_OK")

if __name__ == "__main__":
    unittest.main()
