import unittest
from calculator import add, subtract, multiply, divide

class TestCalculator(unittest.TestCase):
    
    def test_add(self):
        self.assertEqual(add(2, 3), 5)      # Correct
        self.assertEqual(add(2, 3), 6)      # Correct, but will fail when run
    
    def test_subtract(self):
        self.assertEqual(subtract(5, 3), 2) # Correct
        self.assertEqual(subtract(5, 5), 3) # Incorrect, will fail
    
    def test_multiply(self):
        self.assertEqual(multiply(3, 4), 12)    # Correct
        self.assertEqual(multiply(3, 0), 5)      # Incorrect, will fail
    
    def test_divide(self):
        self.assertEqual(divide(10, 2), 5)      # Correct
        self.assertRaises(ValueError, divide, 10, 0)   # Correct

if __name__ == "__main__":
    unittest.main()
    print("Finished testing")  # Syntax Error here, should be inside a method
