using System;
using System.IO;
using System.Collections.Generic;
using Xunit;
using SlackerRunner;
using SlackerRunner.Util;


namespace DatabaseUnittests
{
    public class AllTestSpecs
    {
        // Relative path to test dir 
        public static string RUN_TEST_DIR = Path.GetFullPath(Path.Combine("..", "..", "..", "TestSpecs")) + "\\";
        public static string SPEC_TEST_DIR = Path.GetFullPath(Path.Combine("..", "..", "..", "TestSpecs", "spec") + "/");


        /// <summary>
        /// Runs Slacker spec tests 
        /// </summary>
        [Theory, MemberData("TestFiles", typeof(SpecTestFile))]
        public void runSpecs(ISpecTestFile File)
        {
            SlackerResults SlackerResults = new SlackerService().Run(RUN_TEST_DIR, SPEC_TEST_DIR + File.FileName);
            Assert.True(SlackerResults.Passed, SlackerResults.Message);
        }


        /// <summary>
        /// Runs Slacker spec tests 
        /// </summary>
        [Theory, MemberData("TestFiles", typeof(IndividualSpecTestFile))]
        public void runSpecsIndividually(ISpecTestFile File)
        {
            SlackerResults SlackerResults = new SlackerService().Run(RUN_TEST_DIR, SPEC_TEST_DIR + File.FileName);
            Assert.True(SlackerResults.Passed, SlackerResults.Message);
        }

        /// <summary>
        /// Uses the SpecTesterResolver to figure out all the test files in a directory
        /// </summary>
        public static IEnumerable<object[]> TestFiles(Type type)
        {
            // Pass either SpecTestFile to run tests in a group or IndividualTestFile to run one test file at a time 
            List<ISpecTestFile> files = SpecsTesterResolver.ProcessDirectory(SPEC_TEST_DIR, type);

            // Back to caller
            foreach (ISpecTestFile file in files)
                yield return new object[] { file };
        }

    }
}