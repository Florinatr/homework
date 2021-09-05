#include <memory>
#include <stdexcept>
#include <vector>


namespace scudb {

    /**
     * The Matrix type defines a common
     * interface for matrix operations.
     */
    template <typename T>
    class Matrix {
    protected:
        /**
         * TODO (P0): Add implementation
         *
         * Construct a new Matrix instance.
         * @param rows The number of rows
         * @param cols The number of columns
         *
         */
        Matrix(int rows, int cols) : rows_(rows), cols_(cols)////
        {
            linear_ = new T[rows * cols];
        }

        /** The number of rows in the matrix */
        int rows_;
        /** The number of columns in the matrix */
        int cols_;

        /**
         * TODO(P0): Allocate the array in the constructor.
         * TODO(P0): Deallocate the array in the destructor.
         * A flattened array containing the elements of the matrix.
         */
        T* linear_;

    public:
        /** @return The number of rows in the matrix */
        virtual int GetRowCount() const = 0;

        /** @return The number of columns in the matrix */
        virtual int GetColumnCount() const = 0;

        /**
         * Get the (i,j)th matrix element.
         *
         * Throw OUT_OF_RANGE if either index is out of range.
         *
         * @param i The row index
         * @param j The column index
         * @return The (i,j)th matrix element
         * @throws OUT_OF_RANGE if either index is out of range
         */
        virtual T GetElement(int i, int j) const = 0;

        /**
         * Set the (i,j)th matrix element.
         *
         * Throw OUT_OF_RANGE if either index is out of range.
         *
         * @param i The row index
         * @param j The column index
         * @param val The value to insert
         * @throws OUT_OF_RANGE if either index is out of range
         */
        virtual void SetElement(int i, int j, T val) = 0;

        /**
         * Fill the elements of the matrix from `source`.
         *
         * Throw OUT_OF_RANGE in the event that `source`
         * does not contain the required number of elements.
         *
         * @param source The source container
         * @throws OUT_OF_RANGE if `source` is incorrect size
         */
        virtual void FillFrom(const std::vector<T>& source) = 0;

        /**
         * Destroy a matrix instance.
         * TODO(P0): Add implementation
         */
        virtual ~Matrix()
        {
            delete[] linear_;
        }
    };

    /**
     * The RowMatrix type is a concrete matrix implementation.
     * It implements the interface defined by the Matrix type.
     */
    template <typename T>
    class RowMatrix : public Matrix<T> {
    public:
        /**
         * TODO(P0): Add implementation
         *
         * Construct a new RowMatrix instance.
         * @param rows The number of rows
         * @param cols The number of columns
         */
        RowMatrix(int rows, int cols) : Matrix<T>(rows, cols)
        {
            data = new T * [rows];
            for (int i = 0; i != rows; ++i)
            {
                data[i] = this->linear_ + i * cols;
            }
        }

        /**
         * TODO(P0): Add implementation
         * @return The number of rows in the matrix
         */
        int GetRowCount() const override { return this->rows_; }

        /**
         * TODO(P0): Add implementation
         * @return The number of columns in the matrix
         */
        int GetColumnCount() const override { return this->cols_; }

        /**
         * TODO(P0): Add implementation
         *
         * Get the (i,j)th matrix element.
         *
         * Throw OUT_OF_RANGE if either index is out of range.
         *
         * @param i The row index
         * @param j The column index
         * @return The (i,j)th matrix element
         * @throws OUT_OF_RANGE if either index is out of range
         */
        T GetElement(int i, int j) const override
        {
            if (i < 0 || i >= this->rows_ || j < 0 || j >= this->cols_)
            {
                std::cout << "out of range" << std::endl;
                return 0;
            }
            else
            {
                return data[i][j];
            }
            // throw NotImplementedException{"RowMatrix::GetElement() not implemented."};
        }

        /**
         * Set the (i,j)th matrix element.
         *
         * Throw OUT_OF_RANGE if either index is out of range.
         *
         * @param i The row index
         * @param j The column index
         * @param val The value to insert
         * @throws OUT_OF_RANGE if either index is out of range
         */
        void SetElement(int i, int j, T val) override
        {
            if (i < 0 || i >= this->rows_ || j < 0 || j >= this->cols_)
            {
                std::cout << "out of range" << std::endl;

            }
            else
            {
                data[i][j] = val;
            }
        }

        /**
         * TODO(P0): Add implementation
         *
         * Fill the elements of the matrix from `source`.
         *
         * Throw OUT_OF_RANGE in the event that `source`
         * does not contain the required number of elements.
         *
         * @param source The source container
         * @throws OUT_OF_RANGE if `source` is incorrect size
         */
        void FillFrom(const std::vector<T>& source) override
        {
            if (source.size() != this->rows_ * this->cols_)
            {
                std::cout << "out of range" << std::endl;
            }
            else
            {
                for (int idx = 0; idx != source.size(); ++idx)
                {
                    this->linear_[idx] = source[idx];
                }
            }
            // throw NotImplementedException{"RowMatrix::FillFrom() not implemented."};
        }

        /**
         * TODO(P0): Add implementation
         *
         * Destroy a RowMatrix instance.
         */
        ~RowMatrix() override
        {
            delete[] data;
        }

    private:
        /**
         * A 2D array containing the elements of the matrix in row-major format.
         *
         * TODO(P0):
         * - Allocate the array of row pointers in the constructor.
         * - Use these pointers to point to corresponding elements of the `linear` array.
         * - Don't forget to deallocate the array in the destructor.
         */
        T** data;
    };

    /**
     * The RowMatrixOperations class defines operations
     * that may be performed on instances of `RowMatrix`.
     */
    template <typename T>
    class RowMatrixOperations {
    public:
        /**
         * Compute (`matrixA` + `matrixB`) and return the result.
         * Return `nullptr` if dimensions mismatch for input matrices.
         * @param matrixA Input matrix
         * @param matrixB Input matrix
         * @return The result of matrix addition
         */
        static std::unique_ptr<RowMatrix<T>> Add(const RowMatrix<T>* matrixA, const RowMatrix<T>* matrixB)
        {
            // TODO(P0): Add implementation
            if (!matrixA || !matrixB || matrixA->GetRowCount() != matrixB->GetRowCount() || matrixA->GetColumnCount() != matrixB->GetColumnCount())
            {   
                return std::unique_ptr<RowMatrix<T>>(nullptr);
            }
            else
            {
                int row = matrixA->GetRowCount();
                int col = matrixA->GetColumnCount();
                auto result = std::make_unique<RowMatrix<T>>(row, col);
                for (int i = 0; i < row; ++i)
                {
                    for (int j = 0; j < col; ++j)
                    {
                        result->SetElement(i, j, matrixA->GetElement(i, j) + matrixB->GetElement(i, j));
                    }
                }
                return result;
            }
        }

        /**
         * Compute the matrix multiplication (`matrixA` * `matrixB` and return the result.
         * Return `nullptr` if dimensions mismatch for input matrices.
         * @param matrixA Input matrix
         * @param matrixB Input matrix
         * @return The result of matrix multiplication
         */
        static std::unique_ptr<RowMatrix<T>> Multiply(const RowMatrix<T>* matrixA, const RowMatrix<T>* matrixB) {
            // TODO(P0): Add implementation
            if (!matrixA || !matrixB || matrixA->GetColumnCount() != matrixB->GetRowCount())
            {
                return std::unique_ptr<RowMatrix<T>>(nullptr);
            }
            else
            {
                int row = matrixA->GetRowCount();
                int mid = matrixA->GetColumnCount();
                int col = matrixB->GetColumnCount();
                auto  result = std::make_unique<RowMatrix<T>>(row, col);
                for (int i = 0; i < row; ++i)
                {
                    for (int j = 0; j < col; ++j)
                    {
                        T elem = 0;
                        for (int k = 0; k < mid; ++k)
                        {
                            elem = matrixA->GetElement(i, k) * matrixB->GetElement(k, j)+elem;
                        }
                        result->SetElement(i, j, elem);
                    }
                }
                return  result;
            }
        }

        /**
         * Simplified General Matrix Multiply operation. Compute (`matrixA` * `matrixB` + `matrixC`).
         * Return `nullptr` if dimensions mismatch for input matrices.
         * @param matrixA Input matrix
         * @param matrixB Input matrix
         * @param matrixC Input matrix
         * @return The result of general matrix multiply
         */
        static std::unique_ptr<RowMatrix<T>> GEMM(const RowMatrix<T>* matrixA, const RowMatrix<T>* matrixB,
            const RowMatrix<T>* matrixC)
        {
            // TODO(P0): Add implementation
            if (!matrixA || !matrixB || !matrixC)
            {
                return std::unique_ptr<RowMatrix<T>>(nullptr);
            }
            else
            {
                return Add(Multiply(matrixA, matrixB).get(), matrixC);
            }
        }

        /**
         * Print the Matrix's row, col and values
         *
         * written for test and debug
         *
         * @param matrix Input matrix
         */
        static void Print(const RowMatrix<T>& matrix)
        {
            // printf("Matrix: %d*%d\n", matrix.GetRowCount(), matrix.GetColumnCount());
            for (int i = 0; i < matrix.GetRowCount(); ++i)
            {
                for (int j = 0; j < matrix.GetColumnCount(); ++j)
                {
                    std::cout << matrix.GetElement(i, j) << "  ";
  
                }
                std::cout <<std::endl;
            }
            std::cout << std::endl;
        }
    };

}  // namespace scudb